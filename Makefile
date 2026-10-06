# Every developer-facing target runs inside Docker, so the only host requirements are
# Docker (with Compose v2) and make. Targets prefixed with "_" run *inside* the dev image.

COMPOSE      ?= docker compose
TEST_RUN      = $(COMPOSE) --profile test run --rm --build tests
API          ?= http://localhost:8000

.DEFAULT_GOAL := help
.PHONY: help up down clean logs ps test check lint fmt race-demo scenarios \
        _test-in-container _lint-in-container _check-in-container

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

up: ## Build and boot the whole stack (foreground)
	$(COMPOSE) up --build

down: ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

clean: ## Stop the stack and delete volumes
	$(COMPOSE) --profile test down -v --remove-orphans

logs: ## Follow logs of control plane + worker
	$(COMPOSE) logs -f api outbox-relay consumer worker

ps: ## Show service status/health
	$(COMPOSE) ps

test: ## Run the full test suite with coverage (in Docker)
	$(TEST_RUN) make _test-in-container

lint: ## Formatter check, linter, type checker, security scanner (in Docker)
	$(TEST_RUN) make _lint-in-container

check: ## All quality gates + full test suite (what CI runs)
	$(TEST_RUN) make _check-in-container

fmt: ## Format the code base in place (needs local ruff)
	ruff format src tests scripts alembic && ruff check --fix src tests scripts alembic

race-demo: ## Fire concurrent creates/PATCHes at the running API and show exactly one wins
	$(COMPOSE) up -d --build
	$(COMPOSE) --profile test run --rm --build tests python scripts/race_demo.py --base-url http://api:8000

scenarios: ## Publish duplicate / out-of-order / poison messages straight to the broker
	$(COMPOSE) up -d --build
	$(COMPOSE) stop worker
	$(COMPOSE) --profile test run --rm --build tests python scripts/broker_scenarios.py --base-url http://api:8000; \
	  status=$$?; $(COMPOSE) start worker; exit $$status

# ----------------------------------------------------------------- in-container targets
_test-in-container:
	pytest --cov --cov-report=term-missing --cov-report=xml

_lint-in-container:
	ruff format --check src tests scripts alembic
	ruff check src tests scripts alembic
	mypy
	bandit -q -c pyproject.toml -r src

_check-in-container: _lint-in-container _test-in-container
