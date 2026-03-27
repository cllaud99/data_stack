# PRD — Local Data Stack

## Visão

Plataforma completa de engenharia de dados, rodando 100% local, custo zero, que simula um ambiente de produção real. Objetivo: consolidar skills de engenheiro de dados sênior através da construção e operação de todos os componentes de uma data platform moderna.

---

## Objetivos de Aprendizado

- Dominar orquestração de pipelines com Airflow 3.1+
- Operar um lakehouse real (MinIO + Iceberg + Dremio)
- Aplicar arquitetura medallion (Bronze → Silver → Gold) em dados reais
- Implementar qualidade de dados, catálogo e lineage em produção
- Integrar um agente IA com RAG sobre dados e documentação
- Praticar CI/CD, testes automatizados e GitOps em pipelines de dados

---

## Hardware

| Componente | Especificação |
|---|---|
| CPU | Intel Core i9 14ª geração |
| RAM | 32GB |
| GPU | RTX 5070 Ti — 16GB VRAM |
| Storage | Local (SSD recomendado) |

---

## Arquitetura Geral

```
┌──────────────────────────────────────────────────────────────┐
│                    CONSUMO & DISCOVERY                       │
│         Superset · OpenMetadata · Agente IA (FastAPI)        │
├──────────────────────────────────────────────────────────────┤
│                   CAMADA GOLD (Serving)                      │
│                       ClickHouse                             │
├──────────────────────────────────────────────────────────────┤
│                  CAMADA SILVER (Refinada)                    │
│              dbt Core · Great Expectations                   │
├──────────────────────────────────────────────────────────────┤
│                  CAMADA BRONZE (Raw)                         │
│         MinIO (S3) · Apache Iceberg · Dremio + Nessie        │
├──────────────────────────────────────────────────────────────┤
│                      INGESTÃO                                │
│      APIs Públicas · Datasets Públicos · Webhooks            │
│              Airbyte OSS · Python Connectors                 │
├──────────────────────────────────────────────────────────────┤
│                    STREAMING                                 │
│                      Redpanda                                │
├──────────────────────────────────────────────────────────────┤
│                   ORQUESTRAÇÃO                               │
│                     Airflow 3+                               │
├──────────────────────────────────────────────────────────────┤
│               OBSERVABILIDADE & CI/CD                        │
│           Grafana · Prometheus · GitHub Actions              │
└──────────────────────────────────────────────────────────────┘
```

---

## Stack Completa

### Ingestão
| Ferramenta | Papel |
|---|---|
| Airbyte OSS | Conectores para APIs e datasets públicos |
| Python custom connectors | APIs sem conector nativo no Airbyte |
| FastAPI | Receptor de webhooks |
| Redpanda | Broker de streaming (Kafka-compatible, sem JVM) |

### Storage & Lakehouse
| Ferramenta | Papel |
|---|---|
| MinIO | Object storage S3-compatible (camadas Bronze/Silver) |
| Apache Iceberg | Formato de tabela transacional |
| Dremio Community | Query engine do lakehouse + UI + Project Nessie (versionamento Git-like dos dados) |

### Transformação & Qualidade
| Ferramenta | Papel |
|---|---|
| dbt Core | Transformações SQL, testes, documentação, linhagem |
| Great Expectations | Validação e contratos de dados |

### Serving & Visualização
| Ferramenta | Papel |
|---|---|
| ClickHouse | Data warehouse colunares (camada Gold) |
| Apache Superset | Dashboards e exploração de dados |

### Catálogo & Governança
| Ferramenta | Papel |
|---|---|
| OpenMetadata | Catálogo de dados, lineage, glossário de negócio, classificações |

### Observabilidade
| Ferramenta | Papel |
|---|---|
| Prometheus | Coleta de métricas |
| Grafana | Dashboards de saúde da plataforma e pipelines |

### Agente IA
| Ferramenta | Papel |
|---|---|
| Ollama | Runtime local de LLM |
| Qwen2.5-Coder 14B (Q4_K_M) | Modelo — ~9GB VRAM, excelente para SQL e código |
| FastAPI | Interface REST do agente |
| LangChain | Orquestração RAG |
| ChromaDB | Vector store para docs de arquitetura |

### Infraestrutura & CI/CD
| Ferramenta | Papel |
|---|---|
| Docker Compose (profiles) | Orquestração local de serviços |
| GitHub + GitHub Actions | Versionamento e CI/CD |
| dbt tests + Great Expectations | Testes automatizados de dados |

---

## Docker Compose — Perfis

Estratégia de profiles para gerenciar 32GB RAM:

### `core` — sempre rodando
- Airflow 3.1.8: api-server, scheduler, worker, triggerer, dag-processor (separado no 3.x)
- Redis (broker Celery do Airflow)
- PostgreSQL (metadata do Airflow + OpenMetadata + Superset)
- MinIO (buckets: bronze, silver, gold, checkpoints)
- Redpanda + Redpanda Console

**RAM estimada: ~5.5GB**

### `analytics` — plataforma de dados
- Dremio Community
- ClickHouse
- OpenMetadata + Elasticsearch
- Grafana + Prometheus
- Apache Superset

**RAM estimada: ~12GB**

### `ai` — agente inteligente
- Ollama (Qwen2.5-Coder 14B)
- FastAPI (agente REST)
- ChromaDB

**RAM estimada: ~3GB RAM + ~9GB VRAM**

### Cenários de uso
| Perfis ativos | RAM | VRAM | Status |
|---|---|---|---|
| core + analytics | ~18GB | 0 | ✅ tranquilo |
| core + ai | ~9GB | ~9GB | ✅ tranquilo |
| core + analytics + ai | ~21GB | ~9GB | ⚠️ monitorar |

---

## Arquitetura Medallion

```
Bronze (Raw)
│  Dados brutos, sem transformação
│  Formato: Iceberg no MinIO
│  Particionamento: por data de ingestão
│
Silver (Refined)
│  Dados limpos, tipados, validados
│  dbt models + Great Expectations
│  Linhagem registrada no OpenMetadata
│
Gold (Serving)
   Dados agregados e modelados para consumo
   ClickHouse (analytics) + Dremio (ad-hoc)
   Superset consome desta camada
```

---

## Fontes de Dados

Múltiplos domínios para simular um data lake real com origens diversas:

| Domínio | Exemplos de fontes |
|---|---|
| Financeiro | Banco Central API, Yahoo Finance, Binance public API |
| Governo / Brasil | IBGE SIDRA, dados.gov.br, Portal da Transparência |
| Clima | Open-Meteo, INMET |
| Tech | GitHub API, HackerNews API |
| Datasets públicos | Kaggle (download), dados abertos municipais |

---

## Agente IA — Capacidades

### 1. Business Intelligence Conversacional
O agente lê o catálogo do OpenMetadata (schemas, descrições, linhagem) e responde perguntas de negócio em linguagem natural.

**Fluxo:**
```
Pergunta em português
    → Agente consulta OpenMetadata API (contexto das tabelas)
    → LLM gera SQL contextualizado
    → SQL executado no Dremio ou ClickHouse
    → Resultado retornado em linguagem natural
```

**Exemplo:**
> "Qual estado teve maior crescimento de renda em 2024?"
> → SQL gerado → executado → resposta formatada

### 2. Architecture Assistant
O agente lê a documentação interna (PRD, ADRs, padrões de projeto) e responde perguntas sobre a stack.

**Knowledge base:**
- Este PRD
- ADRs (Architecture Decision Records)
- Docs de padrões (nomenclatura, particionamento, etc.)
- README dos pipelines

**Exemplo:**
> "Qual o padrão de nomenclatura das tabelas Silver?"
> "Como funciona o pipeline de ingestão do IBGE?"

---

## Convenções e Padrões

### Nomenclatura de tabelas
```
{camada}_{dominio}_{entidade}

bronze_financeiro_transacoes
silver_financeiro_transacoes
gold_financeiro_resumo_mensal
```

### Estrutura do repositório
```
data_stack/
├── airflow/
│   └── dags/
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── tests/
├── ingestion/
│   ├── connectors/
│   └── webhooks/
├── agent/
│   ├── api/
│   └── rag/
├── infra/
│   └── docker-compose/
├── .github/
│   └── workflows/
└── .llm/
    ├── prd.md
    └── adr/
```

### Padrões de pipeline (Airflow)
- DAG por domínio e frequência: `{dominio}_{frequencia}_{descricao}`
- Exemplo: `financeiro_diario_ingestao_bcb`
- Retries: 3, intervalo exponencial
- Alertas: task callback no Grafana

---

## CI/CD

```
Push / PR no GitHub
    → GitHub Actions
    → dbt test (modelos afetados)
    → Great Expectations (suite do domínio)
    → Build imagens Docker (se alteradas)
    → Deploy local via docker compose pull
```

Testes obrigatórios para merge:
- `dbt test` sem falhas
- Great Expectations suite: 0 critical failures
- Lint SQL (sqlfluff)

---

## Fases de Implementação

### Fase 1 — Fundação 🟡 Em andamento
- [x] Docker Compose com perfil `core` (Airflow 3.1.8, MinIO, Redpanda, PostgreSQL, Redis)
- [x] MinIO configurado com buckets por camada (bronze, silver, gold, checkpoints)
- [x] Primeiro DAG: `financeiro_diario_ingestao_bcb` — 6 séries BCB → Bronze (JSON + Parquet)
- [x] Conector BCB API (`dags/connectors/bcb.py`)
- [x] Utilitários de storage MinIO (`dags/utils/storage.py`)
- [ ] dbt configurado, primeiro modelo Silver

### Fase 2 — Lakehouse ⬜ Pendente
- [ ] Iceberg + Dremio operacional
- [ ] Arquitetura medallion completa (Bronze → Silver → Gold)
- [ ] ClickHouse recebendo camada Gold
- [ ] Great Expectations com suites por domínio

### Fase 3 — Catálogo & Governança ⬜ Pendente
- [ ] OpenMetadata configurado
- [ ] Lineage automático via dbt + OpenMetadata connector
- [ ] Glossário de negócio populado
- [ ] Classificações de dados (PII, público, interno)

### Fase 4 — Streaming ⬜ Pendente
- [ ] Redpanda operacional com primeiros tópicos
- [ ] Webhook receiver (FastAPI) → tópico Redpanda
- [ ] DAG Airflow consumindo tópico → Bronze

### Fase 5 — Visualização & Observabilidade ⬜ Pendente
- [ ] Superset conectado ao ClickHouse e Dremio
- [ ] Dashboards por domínio
- [ ] Grafana + Prometheus monitorando pipelines e infra

### Fase 6 — Agente IA ⬜ Pendente
- [ ] Ollama + Qwen2.5-Coder 14B operacional
- [ ] FastAPI com endpoints de Text2SQL e Architecture Chat
- [ ] RAG sobre OpenMetadata e docs de arquitetura
- [ ] ChromaDB indexando documentação interna

### Fase 7 — CI/CD & Maturidade ⬜ Pendente
- [ ] GitHub Actions com pipeline completo
- [ ] Testes automatizados em todos os domínios
- [ ] ADRs documentando decisões arquiteturais
- [ ] Runbooks operacionais

---

## Decisões Arquiteturais (resumo)

| Decisão | Escolha | Alternativa descartada | Motivo |
|---|---|---|---|
| Query engine lakehouse | Dremio Community | Trino | Mais fácil, UI embutida, Nessie integrado |
| Streaming broker | Redpanda | Kafka | Sem JVM, muito mais leve para uso local |
| DWH serving | ClickHouse | PostgreSQL | Colunares, performance analítica real |
| LLM local | Qwen2.5-Coder 14B | Qwen2.5 32B | 32B não cabe nos 16GB VRAM |
| Automação entre serviços | Sem n8n | n8n | Agente cobre os casos de uso; n8n é overhead |
| Docker profiles | core / analytics / ai | Tudo sempre up | Gerencia 32GB RAM com flexibilidade |

---

*Documento vivo — atualizar a cada decisão arquitetural relevante.*
*ADRs detalhados em `.llm/adr/`*
