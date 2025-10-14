# Tooling commands for Gostage v3 development.

MIGRATE?=migrate
SQLC?=sqlc
MIGRATIONS_DIR=state/migrations
DB?=sqlite3://$(PWD)/state/test.db

.PHONY: sqlc
sqlc:
	cd state/sqlc && $(SQLC) generate

.PHONY: migrate-new
migrate-new:
	@if [ -z "$(name)" ]; then echo "usage: make migrate-new name=<migration_name>"; exit 1; fi
	$(MIGRATE) create -dir $(MIGRATIONS_DIR) -ext sql $(name)

.PHONY: migrate-up
migrate-up:
	$(MIGRATE) -path $(MIGRATIONS_DIR) -database $(DB) up

.PHONY: migrate-down
migrate-down:
	$(MIGRATE) -path $(MIGRATIONS_DIR) -database $(DB) down 1

.PHONY: test
test:
	GOMODCACHE=$$(pwd)/../.gocache/mod GOPATH=$$(pwd)/../.gocache go test ./...
