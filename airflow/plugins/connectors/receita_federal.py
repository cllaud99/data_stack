"""
Conector para o repositório de dados públicos de CNPJ da Receita Federal.
Fonte: Nextcloud público em arquivos.receitafederal.gov.br
Protocolo: WebDAV (share público — sem senha)

Estrutura do repositório:
    /YYYY-MM/
        Empresas0-9.zip          → tabela: empresas
        Estabelecimentos0-9.zip  → tabela: estabelecimentos
        Socios0-9.zip            → tabela: socios
        Simples.zip              → tabela: simples
        Cnaes.zip                → tabela: cnaes
        Motivos.zip              → tabela: motivos
        Municipios.zip           → tabela: municipios
        Naturezas.zip            → tabela: naturezas
        Paises.zip               → tabela: paises
        Qualificacoes.zip        → tabela: qualificacoes
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

import requests

# ─── Configuração WebDAV ───────────────────────────────────────────────────────
_SHARE_TOKEN = "YggdBLfdninEJX9"
_WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
_WEBDAV_AUTH = (_SHARE_TOKEN, "")
_TIMEOUT = 60

# ─── Schemas das tabelas (sem header — Receita Federal) ───────────────────────
# Separador: ";" | Encoding: iso-8859-1 | Quotechar: '"'
SCHEMAS: dict[str, list[str]] = {
    "empresas": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ],
    "estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "email",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ],
    "simples": [
        "cnpj_basico",
        "opcao_pelo_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_pelo_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "cnaes": ["codigo", "descricao"],
    "motivos": ["codigo", "descricao"],
    "municipios": ["codigo", "descricao"],
    "naturezas": ["codigo", "descricao"],
    "paises": ["codigo", "descricao"],
    "qualificacoes": ["codigo", "descricao"],
}


# ─── Mapeamento arquivo → tabela ──────────────────────────────────────────────

def tipo_arquivo(nome: str) -> str:
    """Mapeia o nome do zip para o nome da tabela destino.

    Exemplos:
        Empresas0.zip        → 'empresas'
        Estabelecimentos3.zip → 'estabelecimentos'
        Simples.zip          → 'simples'
    """
    stem = nome.lower().removesuffix(".zip")
    for prefixo, tabela in [
        ("estabelecimentos", "estabelecimentos"),  # verificar antes de 'esta...'
        ("empresas", "empresas"),
        ("socios", "socios"),
        ("simples", "simples"),
        ("cnaes", "cnaes"),
        ("motivos", "motivos"),
        ("municipios", "municipios"),
        ("naturezas", "naturezas"),
        ("paises", "paises"),
        ("qualificacoes", "qualificacoes"),
    ]:
        if stem.startswith(prefixo):
            return tabela
    return stem  # fallback: usa o próprio nome sem extensão


# ─── Cliente WebDAV ────────────────────────────────────────────────────────────

def _propfind(path: str) -> ET.Element:
    url = f"{_WEBDAV_BASE}/{path.strip('/')}"
    if path.strip("/"):
        url += "/"
    resp = requests.request(
        "PROPFIND",
        url,
        auth=_WEBDAV_AUTH,
        headers={"Depth": "1"},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return ET.fromstring(resp.content)


def listar_pastas() -> list[str]:
    """Retorna lista de pastas YYYY-MM ordenadas do mais antigo ao mais recente."""
    ns = {"d": "DAV:"}
    root = _propfind("/")
    pastas = []
    for response in root.findall("d:response", ns):
        href = response.find("d:href", ns).text or ""
        nome = href.rstrip("/").split("/")[-1]
        resourcetype = response.find(".//d:resourcetype", ns)
        is_collection = resourcetype is not None and resourcetype.find("d:collection", ns) is not None
        # Filtra apenas pastas no formato YYYY-MM
        if is_collection and len(nome) == 7 and nome[4] == "-":
            pastas.append(nome)
    return sorted(pastas)


def pasta_mais_recente() -> str:
    """Retorna o nome da pasta mais recente (ex: '2026-03')."""
    pastas = listar_pastas()
    if not pastas:
        raise RuntimeError("Nenhuma pasta encontrada no repositório da Receita Federal")
    return pastas[-1]


def listar_arquivos(pasta: str) -> list[dict]:
    """Retorna lista de dicts com metadados de cada zip na pasta.

    Cada dict contém:
        nome        — nome do arquivo (ex: 'Empresas0.zip')
        pasta       — competência YYYY-MM
        tabela      — nome da tabela destino
        size_bytes  — tamanho original do zip
        url         — URL de download via WebDAV
    """
    ns = {"d": "DAV:"}
    root = _propfind(f"/{pasta}")
    arquivos = []
    for response in root.findall("d:response", ns):
        href = response.find("d:href", ns).text or ""
        nome = href.rstrip("/").split("/")[-1]
        if not nome.lower().endswith(".zip"):
            continue
        size_el = response.find(".//d:getcontentlength", ns)
        arquivos.append({
            "nome": nome,
            "pasta": pasta,
            "tabela": tipo_arquivo(nome),
            "size_bytes": int(size_el.text) if size_el is not None else 0,
            "url": f"{_WEBDAV_BASE}/{pasta}/{nome}",
            "auth_user": _SHARE_TOKEN,
            "auth_pass": "",
        })
    return sorted(arquivos, key=lambda x: x["nome"])
