"""
dremio_init.py — configura Dremio na primeira inicialização.

Executado como serviço dremio-init no Docker Compose (restart: "no").
Idempotente: verifica se cada recurso já existe antes de criar.

Recursos criados:
  1. Usuário admin (bootstrap, apenas se ainda não configurado)
  2. Source S3 "minio"   — acesso direto aos buckets bronze/landing/silver/gold
  3. Source Nessie "nessie" — catalog Iceberg apontando para s3://warehouse/
  4. VDS bronze_cnpj     — views em data_stack/bronze_cnpj/ abstraindo os paths Bronze
"""

from __future__ import annotations

import logging
import os
import sys
import time
from urllib.parse import quote

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dremio-init")

# ─── Configuração via env ──────────────────────────────────────────────────────
DREMIO_URL          = os.environ["DREMIO_URL"].rstrip("/")
ADMIN_USER          = os.environ.get("DREMIO_ADMIN_USER", "admin")
ADMIN_PASSWORD      = os.environ["DREMIO_ADMIN_PASSWORD"]
MINIO_ENDPOINT      = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY    = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY    = os.environ["MINIO_ROOT_PASSWORD"]
NESSIE_ENDPOINT     = os.environ.get("NESSIE_ENDPOINT", "http://nessie:19120/api/v2")
WAREHOUSE_BUCKET    = os.environ.get("WAREHOUSE_BUCKET", "warehouse")

TIMEOUT = 10

BRONZE_CNPJ_TABLES = [
    "cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes",
    "empresas", "estabelecimentos", "socios", "simples",
]


# ─── HTTP helpers (apiv2) ──────────────────────────────────────────────────────

def _post(path: str, body: dict, token: str | None = None) -> requests.Response:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"_dremio{token}"
    return requests.post(f"{DREMIO_URL}{path}", json=body, headers=headers, timeout=TIMEOUT)


def _put(path: str, body: dict, token: str) -> requests.Response:
    headers = {"Content-Type": "application/json", "Authorization": f"_dremio{token}"}
    return requests.put(f"{DREMIO_URL}{path}", json=body, headers=headers, timeout=TIMEOUT)


def _get(path: str, token: str) -> requests.Response:
    headers = {"Authorization": f"_dremio{token}"}
    return requests.get(f"{DREMIO_URL}{path}", headers=headers, timeout=TIMEOUT)


# ─── HTTP helpers (API v3 — Catalog API) ──────────────────────────────────────

def _get_v3(path: str, token: str) -> requests.Response:
    headers = {"Authorization": f"_dremio{token}"}
    return requests.get(f"{DREMIO_URL}{path}", headers=headers, timeout=TIMEOUT)


def _post_v3(path: str, body: dict, token: str) -> requests.Response:
    headers = {"Content-Type": "application/json", "Authorization": f"_dremio{token}"}
    return requests.post(f"{DREMIO_URL}{path}", json=body, headers=headers, timeout=TIMEOUT)


def _put_v3(path: str, body: dict, token: str) -> requests.Response:
    headers = {"Content-Type": "application/json", "Authorization": f"_dremio{token}"}
    return requests.put(f"{DREMIO_URL}{path}", json=body, headers=headers, timeout=TIMEOUT)


# ─── Aguarda Dremio estar pronto ──────────────────────────────────────────────

def wait_for_dremio(max_wait: int = 300) -> None:
    log.info("Aguardando Dremio em %s ...", DREMIO_URL)
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{DREMIO_URL}/apiv2/login", timeout=5)
            if r.status_code in (200, 401, 403, 405):  # 401/403 = up mas sem auth
                log.info("Dremio disponível.")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    raise RuntimeError(f"Dremio não respondeu em {max_wait}s")


# ─── Bootstrap (primeiro usuário) ─────────────────────────────────────────────

def bootstrap() -> None:
    """Cria o primeiro admin se o Dremio ainda não foi configurado."""
    import time as _time
    headers = {
        "Content-Type": "application/json",
        "Authorization": "_dremionull",
    }
    r = requests.put(
        f"{DREMIO_URL}/apiv2/bootstrap/firstuser",
        json={
            "userName": ADMIN_USER,
            "firstName": "Admin",
            "lastName": "Stack",
            "email": "admin@datastack.local",
            "createdAt": int(_time.time() * 1000),
            "password": ADMIN_PASSWORD,
        },
        headers=headers,
        timeout=TIMEOUT,
    )
    if r.status_code == 200:
        log.info("Admin criado: %s", ADMIN_USER)
    elif r.status_code == 409:
        log.info("Bootstrap já realizado — admin existe.")
    else:
        log.warning("Bootstrap respondeu %s: %s", r.status_code, r.text[:300])


# ─── Login ────────────────────────────────────────────────────────────────────

def login() -> str:
    r = _post("/apiv2/login", {"userName": ADMIN_USER, "password": ADMIN_PASSWORD})
    r.raise_for_status()
    token = r.json()["token"]
    log.info("Login OK — token obtido.")
    return token


# ─── Sources ──────────────────────────────────────────────────────────────────

def source_exists(name: str, token: str) -> bool:
    r = _get(f"/apiv2/source/{name}/", token)
    return r.status_code == 200


def create_minio_source(token: str) -> None:
    name = "minio"
    if source_exists(name, token):
        log.info("Source '%s' já existe — pulando.", name)
        return

    minio_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    payload = {
        "entityType": "source",
        "name": name,
        "type": "S3",
        "config": {
            "credentialType": "ACCESS_KEY",
            "accessKey": MINIO_ACCESS_KEY,
            "accessSecret": MINIO_SECRET_KEY,
            "secure": False,
            "compatibilityMode": True,
            "externalBucketList": ["bronze", "landing", "silver", "gold", "warehouse"],
            "rootPath": "/",
            "defaultCtasFormat": "PARQUET",
            "isCachingEnabled": False,
            "propertyList": [
                {"name": "fs.s3a.endpoint",              "value": minio_host},
                {"name": "fs.s3a.path.style.access",     "value": "true"},
                {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
            ],
        },
    }
    r = _put(f"/apiv2/source/{name}/", payload, token)
    if r.status_code in (200, 201):
        log.info("Source '%s' criada.", name)
    else:
        log.error("Falha ao criar source '%s': %s — %s", name, r.status_code, r.text[:300])


def create_nessie_source(token: str) -> None:
    name = "nessie"
    if source_exists(name, token):
        log.info("Source '%s' já existe — pulando.", name)
        return

    minio_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    payload = {
        "entityType": "source",
        "name": name,
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": NESSIE_ENDPOINT,
            "nessieAuthType": "NONE",
            "awsRootPath": WAREHOUSE_BUCKET,
            "credentialType": "ACCESS_KEY",
            "awsAccessKey": MINIO_ACCESS_KEY,
            "awsAccessSecret": MINIO_SECRET_KEY,
            "secure": False,
            "propertyList": [
                {"name": "fs.s3a.path.style.access",          "value": "true"},
                {"name": "fs.s3a.endpoint",                   "value": minio_host},
                {"name": "fs.s3a.connection.ssl.enabled",     "value": "false"},
                {"name": "dremio.s3.compat",                  "value": "true"},
            ],
        },
    }
    r = _put(f"/apiv2/source/{name}/", payload, token)
    if r.status_code in (200, 201):
        log.info("Source '%s' criada.", name)
    else:
        log.error("Falha ao criar source '%s': %s — %s", name, r.status_code, r.text[:300])


# ─── Catalog API v3 — helpers ─────────────────────────────────────────────────

def get_entity_by_path(token: str, path: list[str]) -> dict | None:
    """Busca entidade no Catalog API pelo path. Retorna None se não encontrada."""
    path_str = "/".join(quote(p, safe="") for p in path)
    r = _get_v3(f"/api/v3/catalog/by-path/{path_str}", token)
    if r.status_code == 200:
        return r.json()
    return None


def create_space(token: str, name: str) -> None:
    """Cria um Space no Dremio se não existir."""
    if get_entity_by_path(token, [name]):
        log.info("Space '%s' já existe.", name)
        return
    r = _post_v3("/api/v3/catalog", {"entityType": "space", "name": name}, token)
    if r.status_code in (200, 201):
        log.info("Space '%s' criado.", name)
    else:
        log.error("Falha ao criar space '%s': %s — %s", name, r.status_code, r.text[:200])


def create_folder(token: str, path: list[str]) -> None:
    """Cria uma pasta dentro de um Space se não existir."""
    if get_entity_by_path(token, path):
        log.info("Folder %s já existe.", path)
        return
    r = _post_v3("/api/v3/catalog", {"entityType": "folder", "path": path}, token)
    if r.status_code in (200, 201):
        log.info("Folder %s criado.", path)
    else:
        log.error("Falha ao criar folder %s: %s — %s", path, r.status_code, r.text[:200])


def promote_to_physical_dataset(token: str, path: list[str]) -> bool:
    """Promove uma pasta Bronze como dataset Parquet via folder_format (apiv2).

    Usa GET + PUT /apiv2/source/{source}/folder_format/{path} — o endpoint
    que o UI do Dremio usa ao clicar em 'Format Folder' e salvar.

    Detalhes:
    - Segmentos com caracteres especiais (ex: =) são URL-encoded individualmente
    - PUT body = apenas o objeto 'fileFormat' do GET (sem o wrapper externo)
    - O Catalog v3 PUT /api/v3/catalog/{id} NÃO funciona para folders em sources
      S3 porque os IDs são virtuais e o endpoint só aceita datasets existentes.

    Retorna True se promovida (ou já estava). False se o path não existir no S3.
    """
    source_name = path[0]
    folder_segments = path[1:]

    # Cada segmento é URL-encoded individualmente (%3D para = em tabela=X)
    encoded_path = "/".join(quote(seg, safe="") for seg in folder_segments)
    format_url = f"/apiv2/source/{source_name}/folder_format/{encoded_path}"

    # GET: descobre o formato atual (Dremio detecta Parquet automaticamente)
    r_get = _get(format_url, token)
    if r_get.status_code == 404:
        log.warning(
            "Pasta não encontrada no source '%s': %s — "
            "dado Bronze ausente no MinIO ou path incorreto.",
            source_name, folder_segments,
        )
        return False
    if r_get.status_code != 200:
        log.warning("GET folder_format %s: %s — %s", path, r_get.status_code, r_get.text[:200])
        return False

    file_format = r_get.json().get("fileFormat", {})
    if file_format.get("type") and file_format["type"] != "Unknown":
        # Já tem formato definido — re-salva para garantir promoção efetiva
        log.info("Pasta %s já detectada como %s — confirmando promoção.", path, file_format["type"])

    # Garante tipo Parquet e salva (PUT com o fileFormat dict diretamente)
    file_format["type"] = "Parquet"
    r_put = _put(format_url, file_format, token)
    if r_put.status_code in (200, 201):
        log.info("Pasta %s promovida como Parquet.", path)
        return True

    log.warning(
        "Falha ao promover %s: %s — %s.",
        path, r_put.status_code, r_put.text[:300],
    )
    return False


def create_vds(token: str, path: list[str], sql: str) -> None:
    """Cria um Virtual Dataset (view) no Dremio. Idempotente."""
    entity = get_entity_by_path(token, path)
    if entity and entity.get("type") == "VIRTUAL_DATASET":
        log.info("VDS %s já existe.", path)
        return
    payload = {
        "entityType": "dataset",
        "type": "VIRTUAL_DATASET",
        "path": path,
        "sql": sql,
    }
    r = _post_v3("/api/v3/catalog", payload, token)
    if r.status_code in (200, 201):
        log.info("VDS %s criado.", path)
    else:
        log.error("Falha ao criar VDS %s: %s — %s", path, r.status_code, r.text[:300])


# ─── Bronze CNPJ — VDS layer ──────────────────────────────────────────────────

def setup_bronze_cnpj_views(token: str) -> None:
    """Cria VDS em data_stack/bronze_cnpj/ para cada tabela Bronze CNPJ.

    Fluxo por tabela:
      1. Promove tabela=X folder no Bronze como PHYSICAL_DATASET (Parquet)
      2. Cria VDS data_stack/bronze_cnpj/{tabela} → SELECT * FROM bronze promovido

    Tabelas sem dado Bronze ainda (Airflow não rodou) são puladas com warning.
    Rodar novamente após ingestão para criar os VDS pendentes.
    """
    create_space(token, "data_stack")
    create_folder(token, ["data_stack", "bronze_cnpj"])

    criados = 0
    pulados = 0
    for table in BRONZE_CNPJ_TABLES:
        bronze_path = ["minio", "bronze", "receita_federal", "cnpj", f"tabela={table}"]
        if not promote_to_physical_dataset(token, bronze_path):
            pulados += 1
            continue
        create_vds(
            token,
            path=["data_stack", "bronze_cnpj", table],
            sql=f'SELECT * FROM minio.bronze."receita_federal".cnpj."tabela={table}"',
        )
        criados += 1

    log.info("Bronze CNPJ VDS: %d criados, %d pulados (Bronze ausente).", criados, pulados)
    if pulados:
        log.warning(
            "%d tabela(s) sem dado Bronze — rode 'make dbt-promote-bronze' "
            "após a ingestão do Airflow para criar os VDS pendentes.",
            pulados,
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    wait_for_dremio()
    bootstrap()
    time.sleep(2)
    token = login()
    create_minio_source(token)
    create_nessie_source(token)
    setup_bronze_cnpj_views(token)
    log.info("✅ Dremio configurado com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Falha na inicialização do Dremio: %s", exc)
        sys.exit(1)
