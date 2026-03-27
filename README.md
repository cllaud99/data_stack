# Local Data Stack

Plataforma completa de engenharia de dados rodando 100% local, custo zero. Simula um ambiente enterprise real para consolidação de skills de engenheiro de dados sênior.

Documentação completa: [`.llm/prd.md`](.llm/prd.md) · Arquitetura: [`.llm/architecture.html`](.llm/architecture.html) · Conhecimentos: [`.llm/conhecimentos.md`](.llm/conhecimentos.md)

---

## Pré-requisitos

- Docker + Docker Compose v2
- Python 3.12+ com `uv`
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
make analytics-up   # core + Dremio, Nessie, ClickHouse, OpenMetadata, Superset, Grafana
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
| Nessie | http://localhost:19120 | — |
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
│   ├── Dockerfile              # Imagem customizada apache/airflow:3.1.8 (uv)
│   ├── requirements.txt        # boto3, pandas, pyarrow, requests
│   ├── dags/
│   │   ├── cnpj_ingestao_receita_federal.py  # DAG mensal: CNPJ → landing + bronze
│   │   ├── hello_world.py                    # DAG de smoke test
│   │   └── hello_minio.py                    # DAG de teste MinIO
│   └── plugins/
│       ├── connectors/
│       │   └── receita_federal.py  # WebDAV client + schemas CNPJ
│       └── utils/
│           └── storage.py          # Helpers MinIO (upload, download, Parquet, landing/bronze keys)
├── infra/
│   ├── docker-compose.yml      # 3 profiles: core / analytics / ai
│   ├── .env                    # Gerado via make setup (não versionado)
│   ├── .env.example            # Template de variáveis
│   ├── config/                 # Prometheus, Grafana datasources, Redpanda Console
│   └── scripts/                # init-databases.sh (PostgreSQL)
├── .llm/
│   ├── prd.md                  # Product Requirements Document
│   ├── architecture.html       # Diagrama visual da stack
│   └── conhecimentos.md        # Registro de aprendizados técnicos
└── Makefile                    # Comandos de desenvolvimento
```

---

## Pipelines ativos

| DAG | Frequência | Fonte | Destino |
|---|---|---|---|
| `cnpj_ingestao_receita_federal` | Mensal (dia 20) | Receita Federal WebDAV | landing → bronze |

### Camadas de dados (MinIO)

| Bucket | Formato | Conteúdo |
|---|---|---|
| `landing` | ZIP original | Arquivos brutos da Receita Federal (audit trail) |
| `bronze` | Parquet/Snappy | Dados extraídos, particionados por tabela e competência |

#### Estrutura bronze CNPJ

```
bronze/receita_federal/cnpj/
  tabela=empresas/competencia=2026-03/part=0000/data.parquet
  tabela=estabelecimentos/competencia=2026-03/part=0000/data.parquet
  tabela=socios/competencia=2026-03/part=0000/data.parquet
  tabela=simples/competencia=2026-03/part=0000/data.parquet
  tabela=cnaes/competencia=2026-03/part=0000/data.parquet
  ... (10 tabelas, 37 arquivos zip por competência)
```

---

## Status de implementação

| Fase | Status | Descrição |
|---|---|---|
| 1 — Fundação | 🟢 Concluída | Infra core, pipeline CNPJ end-to-end (landing + bronze) |
| 2 — Lakehouse | 🟡 Em andamento | Dremio + Nessie up, dbt + Iceberg Silver pendente |
| 3 — Catálogo | ⬜ Pendente | OpenMetadata |
| 4 — Streaming | ⬜ Pendente | Redpanda pipelines |
| 5 — Visualização | ⬜ Pendente | Superset + Grafana dashboards |
| 6 — Agente IA | ⬜ Pendente | Ollama + RAG |
| 7 — CI/CD | ⬜ Pendente | GitHub Actions |

---

## Notas operacionais

### WSL2 + Docker — MTU
Containers no WSL2 podem ter falhas de TLS para conexões externas. Fix aplicado nas redes Docker (`com.docker.network.driver.mtu: "1450"`). Requer `docker network prune` + restart dos containers após mudanças de rede.

### Airflow 3.x — variáveis obrigatórias
```yaml
AIRFLOW__API__SECRET_KEY: ${AIRFLOW_SECRET_KEY}          # assina JWTs
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_SECRET_KEY}     # valida JWTs (deve ser igual)
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-webserver:8080/execution/
```

### Backfill CNPJ
```bash
airflow dags backfill -s 2025-01-20 -e 2025-06-20 cnpj_ingestao_receita_federal
```
