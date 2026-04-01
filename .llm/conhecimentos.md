# Conhecimentos — Local Data Stack

> Registro vivo de aprendizados técnicos e arquiteturais acumulados durante a construção da plataforma.
> Separados por tema. Atualize sempre que algo surpreendente ou não óbvio acontecer.

---

## Airflow 3.x — Breaking Changes e Armadilhas

### API migrou para `airflow.sdk`
No Airflow 3.x os decoradores saíram de `airflow.decorators` e foram para o novo SDK:
```python
# Errado (Airflow 2.x)
from airflow.decorators import dag, task

# Correto (Airflow 3.x)
from airflow.sdk import dag, task
```

### JWT Secret compartilhado é obrigatório
O Airflow 3.x tem uma separação real entre componentes: o **scheduler** gera tokens JWT para autorizar execução de tasks, e o **API server** valida esses tokens. Se cada processo gera seu próprio `os.urandom(16)`, os tokens nunca validam — tasks ficam presas em `up_for_retry` com erro `Signature verification failed`, sem nenhum log útil.

**Solução:** adicionar a variável ao `docker-compose.yml` em **todos** os serviços Airflow:
```yaml
AIRFLOW__API__SECRET_KEY: ${AIRFLOW_SECRET_KEY}
AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_SECRET_KEY}
```

### Execution API Server URL precisa apontar para o container
O worker descobre onde está o API server via config. Por padrão tenta `localhost:8080`, que não existe dentro do container do worker.
```yaml
# Chave correta — section "core", não "workers"
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-webserver:8080/execution/
```

### `outlets` e `Asset` não são suportados em todas as versões
`DAG.__init__() got an unexpected keyword argument 'outlets'` — remover `outlets=[...]` e o import de `Asset`. Verificar a versão exata antes de usar features de dataset/asset.

### `plugins/` é o lugar certo para código compartilhado
Colocar `utils/` e `connectors/` dentro de `dags/` é antipattern — o Airflow varre `dags/` buscando DAGs e pode tentar importar tudo. O diretório `plugins/` é adicionado ao `sys.path` automaticamente sem ser varrido por DAGs.

```
airflow/
├── dags/          ← só arquivos de DAG
└── plugins/
    ├── connectors/  ← clientes de APIs externas
    └── utils/       ← helpers de storage, etc.
```

### Depuração progressiva de worker
Quando o worker não executa nenhuma task (logs vazios, state mismatch), usar DAGs de smoke test em ordem crescente de complexidade:
1. `hello_world` — zero imports externos, só logging
2. `hello_minio` — testa comunicação com MinIO
3. DAG real

Nunca debugar a DAG completa quando a infraestrutura base ainda não foi validada.

---

## Docker / WSL2 — Problemas de Rede

### MTU issue no WSL2
Containers Docker no WSL2 herdam o MTU da interface de rede do host Windows (geralmente 1500), mas o tunneling do WSL2 reduz o espaço útil de pacote. Resultado: handshake TLS falha para sites externos porque o `ClientHello` é fragmentado e descartado.

**Sintoma:** `ConnectionError: [Errno 111] Connection refused` ou timeout em HTTPS para hosts externos, enquanto DNS e TCP simples funcionam.

**Fix:** definir MTU 1450 nas redes Docker:
```yaml
networks:
  core-net:
    driver: bridge
    driver_opts:
      com.docker.network.driver.mtu: "1450"
```

Aplicar em **todas** as redes do compose. Recriar containers depois (`docker compose down && docker network prune -f && docker compose up -d`).

**Diagnóstico:**
```bash
# TCP funciona mas HTTPS falha? → MTU
docker exec container python -c "import socket; s = socket.create_connection(('host', 443), 10); print('TCP OK')"
docker exec container python -c "import requests; print(requests.get('https://host', timeout=15).status_code)"
```

### Rate limiting de servidores governamentais
Servidores como o da Receita Federal derrubam conexões simultâneas do mesmo IP. Com `max_active_tis_per_dagrun=4`, 4 downloads paralelos causam `Connection refused` nos excedentes.

**Solução:** reduzir concorrência (`_MAX_CONCURRENT = 2` ou `1`) e adicionar sleep entre requests no início de cada task de download.

---

## Arquitetura Medallion — Decisões

### Landing Zone: append por competência
Para fontes que publicam snapshots mensais (ex: Receita Federal CNPJ), a landing zone deve ser **append por competência** — o arquivo do mês nunca muda após publicação, então sobrescrever por `(fonte, competencia, arquivo)` é seguro e mantém idempotência.

### Bronze: Parquet particionado, Airflow escreve
O Bronze é uma cópia fiel dos dados brutos, sem lógica de negócio. Airflow escreve Parquet com particionamento Hive (`tabela=X/competencia=YYYY-MM/`). Reaplicar idempotência via **delete-before-write** na partição antes de escrever novos arquivos.

### Iceberg entra no Silver, não no Bronze
Overhead de catálogo Iceberg (Nessie) não vale para dados brutos que ninguém consulta diretamente. Iceberg faz sentido onde há:
- Queries concorrentes (Silver/Gold)
- MERGE/upsert (incremental dbt)
- Time travel (auditoria de transformações)
- Schema evolution

### dbt: sources apontam para Bronze, escrita começa no Silver
dbt nunca escreve no Bronze. O Bronze é declarado como `source:` no YAML. O primeiro modelo dbt que escreve algo é `stg_*` (Silver).

### Snapshots dbt: base estável, sem lógica de negócio
dbt snapshots são SCD Type 2 — registram **mudanças de negócio ao longo do tempo**. Se o snapshot for construído em cima de um modelo Silver com regras de negócio, uma mudança nessas regras contamina o histórico retroativamente.

**Padrão seguro:**
```
Bronze → stg_* (só types + rename) → snp_* (snapshot aqui)
                                           ↓
                                      int_* (regras de negócio)
                                           ↓
                                      mrt_* (Gold)
```

Nunca fazer `dbt snapshot --full-refresh` em produção — destrói todo o histórico SCD2.

### ClickHouse é serving, não storage
ClickHouse recebe apenas o **Gold** (marts prontos). Não faz sentido ingerir Bronze ou Silver no ClickHouse — você pagaria o custo de ingestão sem aproveitar o modelo colunar analítico. Sync: Airflow após dbt completar.

### Idempotência por camada

| Camada | Estratégia |
|--------|------------|
| Landing | Overwrite por `(fonte, competencia, arquivo)` — key S3 fixa |
| Bronze | Delete partition → write (Airflow antes de escrever Parquet) |
| Silver | dbt incremental `unique_key` ou full refresh mensal |
| Gold | dbt full refresh (marts) ou incremental `unique_key` |
| ClickHouse | Truncate-and-load por partição ou REPLACE via ENGINE |

---

## Desenvolvimento com Agentes IA (Claude)

### Debugging em camadas, não brute force
Quando um pipeline falha sem logs, a resposta correta não é recriar tudo — é isolar a camada. Exemplo:
- Airflow worker sem logs → não debugar a DAG de 300 linhas, criar um `hello_world`
- `hello_world` funciona → criar `hello_minio`
- `hello_minio` funciona → agora sim debugar a DAG real

O agente tende a querer resolver o problema inteiro de uma vez. Resista.

### Mudanças de infraestrutura exigem `down && up`
Adicionar variáveis de ambiente, trocar configurações de rede ou montar novos volumes só tem efeito após recriar os containers. Muitos ciclos de debugging foram perdidos re-triggerando a DAG sem recriar o ambiente.

### Entender o erro antes de aplicar o fix
`Errno 111 Connection refused` e `TLS handshake timeout` são erros diferentes com causas diferentes. Aplicar um fix de MTU para um `Connection refused` não resolve nada. Diagnosticar primeiro:
1. DNS resolve?
2. TCP conecta?
3. TLS handshake completa?
4. HTTP responde?

### Código compartilhado em projetos Airflow
A estrutura de pastas importa mais do que parece. Airflow 3.x tem comportamento específico para cada pasta montada:
- `dags/` → varrida por DAGs, evitar código não-DAG
- `plugins/` → adicionada ao `sys.path`, ideal para código compartilhado
- `config/` → configurações
- `logs/` → logs de tasks

### Backfill vs DAG params
Para pipelines com competência temporal (meses, trimestres), backfill é o padrão canônico do Airflow — não params manuais. A `logical_date` já carrega a informação de quando a execução foi agendada:
```python
competencia = "{{ logical_date.strftime('%Y-%m') }}"
```

Para reprocessar histórico:
```bash
airflow dags backfill -s 2025-01-20 -e 2025-06-20 nome_da_dag
```

---

## WebDAV / Nextcloud

### PROPFIND com share público
Para shares públicos do Nextcloud, a autenticação WebDAV usa o token do share como usuário e string vazia como senha:
```python
auth = (share_token, "")
requests.request("PROPFIND", url, auth=auth, headers={"Depth": "1"})
```

### Estrutura de URL
```
https://host/public.php/webdav/{pasta}/{arquivo}
```
O `public.php/webdav` é o endpoint WebDAV de shares públicos do Nextcloud (diferente do WebDAV autenticado em `/remote.php/dav`).

---

## Dremio 25.x — API e Configuração IaC

### Bootstrap do primeiro usuário
O endpoint de bootstrap é `PUT /apiv2/bootstrap/firstuser` com header especial `Authorization: _dremionull`. Não é POST, e a autenticação real (token) não existe ainda nesse momento.

```python
requests.put(
    f"{DREMIO_URL}/apiv2/bootstrap/firstuser",
    json={"userName": ..., "firstName": ..., "lastName": ..., "email": ..., "createdAt": int(time()*1000), "password": ...},
    headers={"Content-Type": "application/json", "Authorization": "_dremionull"},
)
```

**Restrições de senha:** mínimo 8 caracteres, ao menos 1 letra e 1 número. `changeme` falha silenciosamente. Use `Admin1234` como padrão no `.env.example`.

Respostas esperadas: `200` (criado), `409` (já existe — idempotente).

### Nomes de campos mudaram entre versões
Na API v2 do Dremio 25.x os nomes de campo para credenciais dependem do *tipo* de source:

| Source type | Campo chave | Campo secret |
|---|---|---|
| `S3` (MinIO) | `accessKey` | `accessSecret` |
| `NESSIE` | `awsAccessKey` | `awsAccessSecret` |

Misturar os campos gera erro 400 sem mensagem clara.

### MinIO como source S3 — campos obrigatórios
`customEndpoint` e `enablePathStyleAccess` foram removidos no Dremio 25.x. O endpoint vai em `propertyList`:

```python
{
    "type": "S3",
    "config": {
        "credentialType": "ACCESS_KEY",
        "accessKey": "...", "accessSecret": "...",
        "compatibilityMode": True,          # obrigatório para MinIO
        "secure": False,
        "externalBucketList": ["bronze", "landing", "silver", "gold", "warehouse"],
        "propertyList": [
            {"name": "fs.s3a.endpoint",              "value": "minio:9000"},
            {"name": "fs.s3a.path.style.access",     "value": "true"},
            {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
        ],
    }
}
```

### Nessie como catalog Iceberg
```python
{
    "type": "NESSIE",
    "config": {
        "nessieEndpoint": "http://nessie:19120/api/v2",
        "nessieAuthType": "NONE",
        "awsRootPath": "warehouse",         # bucket raiz para tabelas Iceberg
        "credentialType": "ACCESS_KEY",
        "awsAccessKey": "...", "awsAccessSecret": "...",
        "secure": False,
        "propertyList": [
            {"name": "fs.s3a.path.style.access",      "value": "true"},
            {"name": "fs.s3a.endpoint",                "value": "minio:9000"},
            {"name": "fs.s3a.connection.ssl.enabled",  "value": "false"},
            {"name": "dremio.s3.compat",               "value": "true"},
        ],
    }
}
```

Nessie aparece vazio no Dremio até que o dbt escreva as primeiras tabelas Iceberg no Silver.

### Healthcheck do Dremio
`curl` não existe na imagem `dremio-oss`. Use verificação de porta:
```yaml
test: ["CMD-SHELL", "ss -tnlp 2>/dev/null | grep -q ':9047' || netstat -tnlp 2>/dev/null | grep -q ':9047'"]
```

`wait_for_dremio()` no script de init deve aceitar `200`, `401`, `403` e `405` — Dremio responde `403` no modo bootstrap e `401` após o admin ser criado.

### Bucket `warehouse` deve existir antes do Nessie
O Nessie usa `s3://warehouse/` para armazenar tabelas Iceberg. Se o bucket não existe quando o Dremio tenta escrever via Nessie, a operação falha. Criar no `minio-init` junto com os outros buckets:
```yaml
mc mb --ignore-existing local/warehouse;
```

### IaC via `dremio-init` container
Padrão adotado: build customizado (`Dockerfile.dremio-init`) em vez de bind mount. No WSL2, bind mounts de arquivos recém-criados falham com "file not found" dentro do container. A imagem construída via `build: context:` sempre tem o arquivo.

```yaml
dremio-init:
  build:
    context: ./scripts
    dockerfile: Dockerfile.dremio-init
  restart: "no"
  depends_on:
    dremio:
      condition: service_started   # wait_for_dremio() faz o polling real
```

---

## Dremio — Consulta a Dados Bronze (S3/MinIO)

### Por que o path é tão longo no Bronze

Dremio trata o MinIO como um **filesystem puro** — cada pasta vira um schema, cada arquivo vira uma tabela. Ele não detecta automaticamente que pastas com padrão `chave=valor` são partições Hive de uma mesma tabela.

Resultado: para consultar um arquivo Bronze particionado você precisa navegar toda a hierarquia:

```sql
SELECT * FROM minio.bronze."receita_federal".cnpj."tabela=cnaes"."competencia=2026-03"."part=0000"."data.parquet"
```

Cada segmento do path é um nível de pasta no MinIO:
```
bucket:     bronze
pasta:      receita_federal/cnpj/
partição:   tabela=cnaes/competencia=2026-03/part=0000/
arquivo:    data.parquet
```

### Como resolver: Promote de pasta no Dremio

É possível clicar com o botão direito em uma pasta no Dremio UI e fazer **"Format Folder"** (ou Promote) para ensiná-lo a ler todas as partições como uma única tabela virtual. Isso funciona para queries ad-hoc no Bronze.

### Por que Iceberg no Silver resolve definitivamente

No Silver (Nessie + Iceberg), o Dremio sabe que é uma tabela — não um conjunto de arquivos. A query vira:

```sql
SELECT * FROM nessie.silver.cnpj_cnaes
```

Sem hierarquia de path, sem saber nada de particionamento físico. Esse é o objetivo do lakehouse: abstrair o layout físico do storage.

**Resumo:** Bronze = arquivos no S3 (path explícito). Silver/Gold = tabelas Iceberg (nome simples). O dbt faz essa promoção.

### Porta 5432 bloqueada no WSL2 Docker Desktop

O proxy de port-forwarding do Docker Desktop às vezes trava e não consegue expiar portas para o host Windows — erro: `ports are not available: /forwards/expose returned unexpected status: 500`.

**Fix adotado:** remover o bind `127.0.0.1:5432:5432` do postgres no compose. Containers internos acessam via nome DNS `postgres:5432`. Acesso externo via:
```bash
docker exec -it data-stack-postgres-1 psql -U postgres
```

---

## dbt-dremio — Configuração do profiles.yml

### O que cada opção do `dbt init` significa

| Opção | Valor escolhido | O que é |
|---|---|---|
| `software_host` | `localhost` | Endereço do Dremio — dentro do Docker seria `dremio`, mas dbt roda no host |
| `port` | `9047` | Porta HTTP do Dremio (UI + API REST que o dbt usa) |
| `user` / `password` | `admin` / `Admin1234` | Credenciais do Dremio |
| `storage_configuration` | `sources_and_spaces` | Dremio OSS — usa sources S3 + Spaces para views. `enterprise_catalog` é só para Dremio Enterprise/Cloud (Arctic) |
| `object_storage_source` | `minio` | Nome do source S3 registrado no Dremio — onde o dbt cria os arquivos físicos (Parquet/Iceberg). Deve bater exatamente com o nome na UI do Dremio |
| `object_storage_path` | `warehouse` | Bucket/pasta dentro do source onde os arquivos são gravados. `warehouse` é o bucket dedicado para Iceberg, alinhado com o Nessie. Sobrescrito por modelo via `config()` |
| `dremio_space` | `data_stack` | Espaço lógico no Dremio onde as **views** são criadas. Separado do storage físico. Permite navegar `data_stack > silver > cnpj > stg_empresas` na UI |
| `dremio_space_folder` | `no_schema` | Pasta padrão dentro do espaço para views. Deixado vazio — cada modelo define a sua via `config()` |
| `threads` | `8` | Paralelismo do dbt. Gargalo real é o Dremio, não o dbt. 8 é confortável para i9 14ª gen |

### Separação física vs lógica no dbt-dremio

```
object_storage_source + object_storage_path  →  onde os arquivos ficam (MinIO)
dremio_space + dremio_space_folder           →  onde as views ficam (Dremio UI)
```

Exemplo de model Gold (Iceberg via Nessie) com config explícita:
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='cnpj',
    object_storage_source='nessie',
    object_storage_path='silver',
    dremio_space='data_stack',
    dremio_space_folder='silver'
) }}
```

### `dremio_space_folder` não aceita barra
O Dremio rejeita folder names com `/` via a API de catalog (400 Bad Request).
Usar sempre nome plano: `dremio_space_folder='silver'`, nunca `'silver/cnpj'`.

### profiles.yml fica em ~/.dbt/ — não no repositório

O `dbt init` gera `~/.dbt/profiles.yml` com credenciais. Não commitar — contém senha.
Solução para onboarding: manter um `dbt/profiles.yml.example` no repo sem credenciais + target `make dbt-setup` que copia e pede as variáveis.

---

## Padrões de Engenharia Adotados

### Dockerfile: `uv` em vez de `pip`
`uv` é 10-100x mais rápido que `pip` para resolver e instalar dependências. Em imagens Docker com muitos pacotes (Airflow), a diferença é significativa no tempo de build.
```dockerfile
RUN pip install --no-cache-dir uv \
    && uv pip install --no-cache \
        "apache-airflow==3.1.8" \
        -r /requirements.txt
```
Sempre pinar a versão do Airflow explicitamente — sem pinar, o `uv` pode resolver para uma versão diferente da imagem base.

### `.env` nunca no git
Segredos (`AIRFLOW_SECRET_KEY`, credenciais MinIO, tokens) ficam em `.env`, que está no `.gitignore`. O `docker-compose.yml` referencia via `${VAR}`. Documentar as variáveis necessárias no `README` ou num `.env.example`.

### Commits semânticos
```
feat: adiciona DAG de ingestão CNPJ Receita Federal
fix: corrige MTU 1450 nas redes Docker para WSL2
refactor: move utils e connectors de dags/ para plugins/
chore: atualiza Dockerfile para usar uv
```

---

---

## dbt-dremio — Escala e Limitações de Memória

### MemoryArbiter do Dremio — limite de hash join em single-node

O `MemoryArbiterImpl` do Dremio controla alocação de memória Arrow (off-heap) para execução de queries. Ele cancela queries que excedem o orçamento disponível, mesmo que o host tenha RAM livre.

**Causa raiz:** O Dremio usa Arrow (columnar, off-heap) para buffers de query. O orçamento total é limitado por `MaxDirectMemorySize` da JVM. Em single-node, com dataset CNPJ nacional:
- `estabelecimentos`: ~60M linhas × ~200 bytes = ~12GB
- `empresas`: ~25M linhas × ~100 bytes = ~2.5GB
- Hash join dessas duas exige construir tabela de hash para o lado menor (~2.5GB) + buffer do probe

Com `MaxDirectMemorySize=8g` e overhead do Dremio, não há memória suficiente para o plano de query.

**Configuração adotada** (não resolve o OOM para datasets grandes, mas é a correta):
```yaml
# docker-compose.yml — DREMIO_JAVA_SERVER_EXTRA_OPTS
-Xmx8g -XX:MaxDirectMemorySize=10g

# infra/config/dremio.conf (montado em /opt/dremio/conf/)
paths.spilling: ["/tmp/dremio/spill"]
```

**Como configurar via dremio-env:** As variáveis `DREMIO_MAX_HEAP_MEMORY_SIZE_MB` e `DREMIO_MAX_DIRECT_MEMORY_SIZE_MB` no `dremio-env` são a forma oficial — `DREMIO_JAVA_SERVER_EXTRA_OPTS` sobrescreve depois mas o startup script pode duplicar flags.

### Arquitetura adotada para dataset CNPJ nacional (~60M linhas)

O dataset CNPJ completo é grande demais para materialização via hash join local.
Padrão adotado:

```
stg_*  →  views    (Bronze → typecast/rename — DDL apenas, dados lidos sob demanda)
int_*  →  views    (join de tabelas FATO grandes — lazy, filter pushdown pelo Dremio)
mrt_*  →  Iceberg  (Gold filtrado por negócio — ex: só ativos, por UF, por CNAE)
```

Exceção: `int_cnpj__socios` (9M linhas, 3 JOINs pequenos) foi materializado como Iceberg incremental no Nessie sem problema.

### Views stg_* — erros de dado aparecem só quando consultadas

`dbt run` em um modelo `materialized='view'` executa apenas `CREATE OR REPLACE VIEW` (DDL).
O dado real nunca é lido. Erros como `TO_DATE` com valor inválido só aparecem quando outra query usa a view.

**Implicação prática:** O `dbt run` dos stg_* passou com sucesso. O erro de `parse_date_br` com valor `'0'` só apareceu quando o `int_*` consultou a view.

**Fix aplicado no `parse_date_br`:**
```sql
{% macro parse_date_br(col) %}
    CASE
        WHEN NULLIF(TRIM({{ col }}), '') IS NULL THEN NULL
        WHEN LENGTH(TRIM({{ col }})) != 8  THEN NULL  -- filtra '0', '00000', etc.
        WHEN TRIM({{ col }}) = '00000000'  THEN NULL
        ELSE TO_DATE(TRIM({{ col }}), 'YYYYMMDD')
    END
{% endmacro %}
```
A Receita Federal usa tanto `'00000000'` quanto `'0'` como placeholder de data nula.

---

*Última atualização: 2026-03-31*
