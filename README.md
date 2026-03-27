# Local Data Stack

Plataforma completa de engenharia de dados rodando 100% local, custo zero. Simula um ambiente enterprise real para consolidação de skills de engenheiro de dados sênior.

Documentação completa: [`.llm/prd.md`](.llm/prd.md)

---

## Pré-requisitos

- Docker + Docker Compose v2
- Python 3.11+ com `uv`
- NVIDIA Container Toolkit (apenas para profile `ai`)

---

## Quick Start

```bash
# 1. Gerar .env com chaves automáticas (apenas na primeira vez)
make setup

# 2. Subir infraestrutura core
make core-up

# 3. Verificar status
make ps
```

### Subir perfis adicionais

```bash
make analytics-up   # core + Dremio, ClickHouse, OpenMetadata, Superset, Grafana
make ai-up          # core + Ollama, ChromaDB, Agent
make up             # tudo
```

---

## Serviços e portas

### Profile `core`

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8090 | admin / changeme |
| MinIO Console | http://localhost:9001 | minioadmin / changeme |
| Redpanda Console | http://localhost:8080 | — |
| PostgreSQL | localhost:5432 | postgres / changeme |

### Profile `analytics`

| Serviço | URL | Credenciais |
|---|---|---|
| Dremio | http://localhost:9047 | (setup na primeira vez) |
| ClickHouse | http://localhost:8123 | default / changeme |
| OpenMetadata | http://localhost:8585 | admin / admin |
| Grafana | http://localhost:3000 | admin / changeme |
| Superset | http://localhost:8088 | admin / changeme |

### Profile `ai`

| Serviço | URL |
|---|---|
| Ollama | http://localhost:11434 |
| ChromaDB | http://localhost:8000 |
| Agent (FastAPI) | http://localhost:8500 |

---

## Estrutura do repositório

```
data_stack/
├── airflow/
│   ├── Dockerfile          # Imagem customizada apache/airflow:3.1.8
│   ├── requirements.txt    # boto3, pandas, pyarrow
│   └── dags/
│       ├── financeiro_diario_bcb.py   # DAG: ingestão BCB → Bronze
│       ├── connectors/bcb.py          # Conector API Banco Central
│       └── utils/storage.py           # Utilitários MinIO (JSON/Parquet)
├── infra/
│   ├── docker-compose.yml  # 3 profiles: core / analytics / ai
│   ├── .env                # Gerado via make setup (não versionado)
│   ├── .env.example        # Template de variáveis
│   ├── config/             # Prometheus, Grafana datasources, Redpanda Console
│   └── scripts/            # init-databases.sh (PostgreSQL)
├── .llm/
│   └── prd.md              # Product Requirements Document
└── Makefile                # Comandos de desenvolvimento
```

---

## Status de implementação

| Fase | Status | Descrição |
|---|---|---|
| 1 — Fundação | 🟡 Em andamento | Infra core rodando, primeiro DAG criado |
| 2 — Lakehouse | ⬜ Pendente | Iceberg + Dremio + ClickHouse |
| 3 — Catálogo | ⬜ Pendente | OpenMetadata |
| 4 — Streaming | ⬜ Pendente | Redpanda pipelines |
| 5 — Visualização | ⬜ Pendente | Superset + Grafana dashboards |
| 6 — Agente IA | ⬜ Pendente | Ollama + RAG |
| 7 — CI/CD | ⬜ Pendente | GitHub Actions |

---

## Notas Airflow 3.x

Breaking changes relevantes já tratados no `docker-compose.yml`:

| Mudança | Antes | Depois |
|---|---|---|
| Auth manager | padrão (SimpleAuth) | `FabAuthManager` via env var |
| Webserver | `airflow webserver` | `airflow api-server` |
| DAG processor | embutido no scheduler | serviço separado `dag-processor` |
| CLI init | `airflow users create` | `python -m airflow users create` |
