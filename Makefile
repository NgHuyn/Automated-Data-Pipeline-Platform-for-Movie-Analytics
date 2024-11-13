include .env

.PHONY: help
help:
	@echo "Choose a command to run:"
	@echo "  make build            - Build all services"
	@echo "  make up               - Start all services"
	@echo "  make down             - Stop all services"
	@echo "  make restart          - Restart all services"
	@echo "  make logs             - View logs for all services"
	@echo "  make to-psql		   - Connect to postgres"
	@echo "  make pgadmin-access   - Access Postgres database via PgAdmin"
	@echo "  make clean            - Remove all stopped services and unused networks"

.PHONY: build
build:
	docker compose build

.PHONY: up
up:
	docker compose up -d

.PHONY: down
down:
	docker compose down

.PHONY: restart
restart:
	docker compose down
	docker compose up -d

.PHONY: logs
logs:
	docker compose logs -f

.PHONY: to-psql
to-psql:
	docker exec -ti postgres_container psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

.PHONY: pgadmin-access
pgadmin-access:
	@echo "Open PgAdmin and use the following details to connect to the Postgres database:"
	@echo "Host: postgres_container"
	@echo "Port: $(POSTGRES_PORT)"
	@echo "Username: $(POSTGRES_USER)"
	@echo "Password: $(POSTGRES_PASSWORD)"
	@echo "Maintenance Database: $(POSTGRES_DB)"

.PHONY: clean
clean:
	docker compose down --volumes --remove-orphans


