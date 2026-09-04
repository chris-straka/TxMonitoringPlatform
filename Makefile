# Bare `make` lists every target that has a `## comment` after its name.
# Cheaper than maintaining a separate help block that drifts out of date.
.DEFAULT_GOAL := help
help:
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# .PHONY tells Make these are cmd names, not files on my hard drive
.PHONY: help fleet dev k8s compose-up up upd down clean build-detector restore test frontend ls migrate db-update db-check db-rows logs infra-up run-api run-worker run-em run-gw infra-validate infra-init infra-plan infra-apply infra-destroy \
	chaos-cloud-down chaos-cloud-up chaos-gateway-kill queue verify e2e demo-help

dev: ## Tilt dev loop
	tilt up

k8s:
	tilt up -f prod.Tiltfile

up: ## docker compose up (foreground)
	docker compose up --remove-orphans

upd: ## docker compose up -d
	docker compose up -d

down: ## docker compose down
	docker compose down

clean: ## docker compose down -v (WIPES all local data, observability history, and dev PKI)
	docker compose down -v

build-detector: ## rebuild the online IID spike-detector configuration in model.zip (amount feature)
	dotnet run --project src/TxMonitoring.ML.DetectorBuilder

# Downloads dependencies for each project (since we're at the root .slnx)
restore:
	dotnet restore
	dotnet tool restore

test: ## build and run all .NET regression tests
	dotnet test TxMonitoringPlatform.slnx

frontend: ## run the Vite dev server
	cd src/TxMonitoring.Web.Dashboard && npm run dev

ls:
	docker compose ls

# This creates the auto-generated C# that you can inspect yourself and apply later
# `make migrate name=MyNewMigration`
migrate: ## make migrate name=MyMigration
	dotnet ef migrations add $(name) --project src/TxMonitoring.Detection.Worker

# If you're trying to migrate an existing field
# 1. Expand (add new field),
# 2. backfill (run a script adding old field data to new field)
# 3. Switch (deploy code that uses new data field with old data field as a fallback)
# 4. Contract (drop the old col after everyone is migrated over)

# In production, we would use CI/CD to apply the SQL migration
db-update: ## apply pending EF migrations
	dotnet ef database update --project src/TxMonitoring.Detection.Worker

# -d DB -c command \dt describe tables
db-check:
	docker exec -it txmonitoringplatform-postgres-1 psql -U admin -d tx_monitoring -c "\dt"

db-rows:
	docker exec -it txmonitoringplatform-postgres-1 psql -U admin -d tx_monitoring -c "SELECT * FROM \"Transactions\";"

logs:
	docker compose logs -f $(c)

# Start only backing infrastructure dependencies
infra-up:
	docker compose up -d postgres kafka kafka-init otel-collector loki prometheus tempo

run-api:
	dotnet run --project src/TxMonitoring.Ingestion.Api

run-worker:
	dotnet run --project src/TxMonitoring.Detection.Worker

run-em:
	dotnet run --project src/TxMonitoring.Feed.Emulator

run-gw:
	dotnet run --project src/TxMonitoring.Capture.Gateway

# ---------------------------------------------------------------------------
# Store-and-forward demo
#
# The point: the feed keeps generating through a total ingestion outage, the gateway
# survives its own restart mid-outage, and when ingestion returns the queue
# drains every transaction the gateway accepted, without duplicate Postgres rows.
#
# Watch `capture_queue_depth` and `capture_oldest_transaction_age_seconds` in Grafana
# (localhost:3000) while running these.
# ---------------------------------------------------------------------------

demo-help: ## print the store-and-forward outage demo, step by step
	@echo "1. make upd                  # everything comes up, queue depth ~0"
	@echo "2. make chaos-cloud-down     # kill the cloud; queue starts climbing"
	@echo "3. make queue                # confirm the buffer is filling"
	@echo "4. make chaos-gateway-kill   # kill the gateway TOO, mid-outage"
	@echo "5. make queue                # buffer survived the restart"
	@echo "6. make chaos-cloud-up       # cloud returns; queue drains oldest-first"
	@echo "7. make verify               # quiesce, drain queue/Kafka/outbox, then audit"

# The cloud dies. Sensors and gateway keep running.
fleet: ## start extra feed-emulator containers (3 replicas, ACCT-0000..ACCT-0011)
	docker compose --profile fleet up -d

chaos-cloud-down: ## kill ingestion; the capture queue starts filling
	docker compose stop ingestion-api

chaos-cloud-up: ## bring ingestion back; the queue drains
	docker compose start ingestion-api

# The gateway dies too, while ingestion is still down. This is the interesting one:
# the SQLite buffer is on a named volume, so the queue is still there on restart.
chaos-gateway-kill: ## restart the gateway mid-outage; the buffer survives
	docker compose restart capture-gateway

# How deep is the local buffer right now?
queue: ## current capture buffer depth
	@curl -s http://localhost:5272/buffer | python3 -m json.tool

# Quiesces and later restores any running feed containers so the queue, Kafka lag, and outbox
# checks describe one stable snapshot instead of racing newly generated transactions.
verify: ## drain and audit IDs, duplicates, sequence gaps, and lag
	@./scripts/verify.sh

e2e: ## run the isolated Docker failure/recovery suite
	./scripts/e2e.sh

# The Terraform directory configures real external clusters. Validation is local and read-only;
# state initialization and mutation require the caller to name every input explicitly.
infra-validate: ## statically validate Terraform without contacting its S3 backend
	cd infra && terraform fmt -check -recursive
	cd infra && terraform init -backend=false -input=false
	cd infra && terraform validate

infra-init: ## initialize Terraform; make infra-init backend=/secure/backend.hcl
	@test -n "$(backend)" || (echo "usage: make infra-init backend=/secure/backend.hcl" >&2; exit 2)
	cd infra && terraform init -backend-config="$(backend)"

infra-plan: ## save a plan; make infra-plan vars=prod.tfvars plan=tfplan
	@test -n "$(vars)" || (echo "usage: make infra-plan vars=prod.tfvars plan=tfplan" >&2; exit 2)
	@test -n "$(plan)" || (echo "usage: make infra-plan vars=prod.tfvars plan=tfplan" >&2; exit 2)
	cd infra && terraform plan -var-file="$(vars)" -out="$(plan)"

infra-apply: ## apply a reviewed saved plan; make infra-apply plan=tfplan
	@test -n "$(plan)" || (echo "usage: make infra-apply plan=tfplan" >&2; exit 2)
	cd infra && terraform apply "$(plan)"

infra-destroy: ## interactively destroy; make infra-destroy vars=prod.tfvars
	@test -n "$(vars)" || (echo "usage: make infra-destroy vars=prod.tfvars" >&2; exit 2)
	cd infra && terraform destroy -var-file="$(vars)"
