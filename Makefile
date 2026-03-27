.DEFAULT_GOAL := help
COMPOSE := docker compose -f infra/docker-compose.yml

# ─── Setup ────────────────────────────────────────────────────────────────────

setup: ## Copia .env.example → infra/.env e gera chaves automáticas
	@if [ ! -f infra/.env ]; then \
		cp infra/.env.example infra/.env; \
		FERNET=$$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"); \
		SECRET=$$(openssl rand -base64 32); \
		SUPERSET=$$(openssl rand -base64 42); \
		UID_VAL=$$(id -u); \
		sed -i "s/^AIRFLOW_FERNET_KEY=$$/AIRFLOW_FERNET_KEY=$$FERNET/" infra/.env; \
		sed -i "s/^AIRFLOW_SECRET_KEY=changeme$$/AIRFLOW_SECRET_KEY=$$SECRET/" infra/.env; \
		sed -i "s/^SUPERSET_SECRET_KEY=$$/SUPERSET_SECRET_KEY=$$SUPERSET/" infra/.env; \
		sed -i "s/^AIRFLOW_UID=1000$$/AIRFLOW_UID=$$UID_VAL/" infra/.env; \
		echo "✓ infra/.env criado com chaves geradas"; \
	else \
		echo "infra/.env já existe — nenhuma alteração feita"; \
	fi

# ─── Core (sempre rodando) ────────────────────────────────────────────────────

core-up: ## Sobe perfil core (Postgres, Redis, Airflow, MinIO, Redpanda)
	$(COMPOSE) --profile core up -d

core-down: ## Para perfil core
	$(COMPOSE) --profile core down

core-logs: ## Logs do perfil core
	$(COMPOSE) --profile core logs -f

# ─── Analytics ────────────────────────────────────────────────────────────────

analytics-up: ## Sobe core + analytics (Dremio, ClickHouse, OpenMetadata, Superset, Grafana)
	$(COMPOSE) --profile core --profile analytics up -d

analytics-down: ## Para core + analytics
	$(COMPOSE) --profile core --profile analytics down

analytics-logs: ## Logs do perfil analytics
	$(COMPOSE) --profile analytics logs -f

# ─── AI ───────────────────────────────────────────────────────────────────────

ai-up: ## Sobe core + ai (Ollama, Agent, ChromaDB) — requer NVIDIA Container Toolkit
	$(COMPOSE) --profile core --profile ai up -d

ai-down: ## Para core + ai
	$(COMPOSE) --profile core --profile ai down

ai-logs: ## Logs do perfil ai
	$(COMPOSE) --profile ai logs -f

# ─── Full stack ───────────────────────────────────────────────────────────────

up: ## Sobe todos os perfis
	$(COMPOSE) --profile core --profile analytics --profile ai up -d

down: ## Para todos os serviços
	$(COMPOSE) --profile core --profile analytics --profile ai down

# ─── Utilitários ──────────────────────────────────────────────────────────────

ps: ## Status de todos os containers
	$(COMPOSE) --profile core --profile analytics --profile ai ps

stats: ## Uso de recursos em tempo real
	docker stats

reset: ## Remove containers e volumes (DESTRUTIVO — apaga todos os dados)
	@echo "ATENÇÃO: isso apaga todos os dados persistidos."
	@read -p "Confirma? [y/N] " confirm && [ "$$confirm" = "y" ]
	$(COMPOSE) --profile core --profile analytics --profile ai down -v

airflow-shell: ## Abre shell no worker do Airflow
	$(COMPOSE) exec airflow-worker bash

minio-shell: ## Abre shell no MinIO (mc)
	$(COMPOSE) exec minio sh

clickhouse-shell: ## Abre CLI do ClickHouse
	$(COMPOSE) exec clickhouse clickhouse-client

help: ## Lista todos os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
