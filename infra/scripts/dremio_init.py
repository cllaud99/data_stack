"""
dremio_init.py — configura Dremio na primeira inicialização.

Executado como serviço dremio-init no Docker Compose (restart: "no").
Idempotente: verifica se cada recurso já existe antes de criar.

Recursos criados:
  1. Usuário admin (bootstrap, apenas se ainda não configurado)
  2. Source S3 "minio"   — acesso direto aos buckets bronze/landing/silver/gold
  3. Source Nessie "nessie" — catalog Iceberg apontando para s3://warehouse/
"""

from __future__ import annotations

import logging
import os
import sys
import time

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


# ─── HTTP helpers ──────────────────────────────────────────────────────────────

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
    """Cria o primeiro admin se o Dremio ainda não foi configurado.

    Requer PUT + Authorization: _dremionull (token especial de bootstrap).
    Senha deve ter >= 8 chars, ao menos 1 letra e 1 número.
    """
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


# ─── Fontes ───────────────────────────────────────────────────────────────────

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


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    wait_for_dremio()
    bootstrap()
    time.sleep(2)   # breve pausa após bootstrap
    token = login()
    create_minio_source(token)
    create_nessie_source(token)
    log.info("✅ Dremio configurado com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Falha na inicialização do Dremio: %s", exc)
        sys.exit(1)
