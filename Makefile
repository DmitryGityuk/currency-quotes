# Shortcuts for macOS, Linux and WSL. On plain Windows use the docker
# compose commands from the README directly; make is not required.

GO ?= go
PLANTUML_VERSION ?= 1.2025.3
PLANTUML_JAR := bin/plantuml-$(PLANTUML_VERSION).jar

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start the stack, wait until it is healthy
	docker compose up --build --detach --wait

.PHONY: down
down: ## Stop the stack
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete its data
	docker compose down --volumes

.PHONY: demo
demo: ## Walk through the assignment against the running stack
	docker compose run --rm demo

.PHONY: observability
observability: ## Start the stack with Prometheus, Grafana and Kafka UI
	docker compose --profile observability up --build --detach --wait

.PHONY: logs
logs: ## Follow the logs of the API and the worker
	docker compose logs --follow api worker

.PHONY: infra
infra: ## Start PostgreSQL and Kafka, create topics, apply migrations
	docker compose up --detach --wait postgres kafka
	docker compose run --rm kafka-init
	docker compose run --rm --build migrate

.PHONY: migrate
migrate: ## Apply database migrations
	docker compose run --rm --build migrate

.PHONY: migrate-down
migrate-down: ## Revert the last migration
	docker compose run --rm --build migrate down 1

.PHONY: run-api
run-api: ## Run the API on the host (needs `make infra`)
	LOG_FORMAT=text $(GO) run ./cmd/quotes api

.PHONY: run-worker
run-worker: ## Run the worker on the host (needs `make infra`)
	HTTP_ADDR=:8081 LOG_FORMAT=text $(GO) run ./cmd/quotes worker

.PHONY: build
build: ## Build the binary into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/quotes ./cmd/quotes

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration tests against PostgreSQL (needs `make infra`)
	TEST_DATABASE_URL='postgres://quotes:quotes@localhost:55432/quotes?sslmode=disable' \
		$(GO) test -tags integration -race -count=1 ./internal/postgres/...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: fmt
fmt: ## Format the code
	golangci-lint fmt

.PHONY: lint-api
lint-api: ## Lint the OpenAPI specification (needs Node.js)
	npx --yes @redocly/cli@1 lint api/openapi.yaml

PROMETHEUS_IMAGE ?= prom/prometheus:v3.15.1

.PHONY: check-alerts
check-alerts: ## Validate the Prometheus config and run the alert rule tests
	docker run --rm --entrypoint /bin/promtool -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" \
		$(PROMETHEUS_IMAGE) check config /etc/prometheus/prometheus.yml
	docker run --rm --entrypoint /bin/promtool -v "$(CURDIR)/deploy/prometheus:/etc/prometheus:ro" -w /etc/prometheus \
		$(PROMETHEUS_IMAGE) test rules alerts_test.yml

$(PLANTUML_JAR):
	mkdir -p bin
	curl --fail --location --silent --show-error --output $@ \
		https://github.com/plantuml/plantuml/releases/download/v$(PLANTUML_VERSION)/plantuml-$(PLANTUML_VERSION).jar

.PHONY: diagrams
diagrams: $(PLANTUML_JAR) ## Render docs/diagrams/*.puml to SVG (needs Java and Graphviz)
	java -jar $(PLANTUML_JAR) -tsvg -failfast2 docs/diagrams/*.puml
