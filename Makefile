# Atalhos de qualidade. Requer Poetry (https://python-poetry.org) no PATH.
# `make check` é o que o engenheiro roda antes de entregar (Definição de Pronto).

POETRY ?= poetry
RUN := PYTHONPATH=src $(POETRY) run
UNIT_MARKERS := not oracle and not rabbitmq and not redis and not load

.PHONY: help install lint format typecheck imports test test-integration check clean

help:  ## Lista os alvos
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

install:  ## Cria .venv e instala dependências travadas
	$(POETRY) install

lint:  ## ruff (lint + formatação, sem alterar)
	$(RUN) ruff check .
	$(RUN) ruff format --check .

format:  ## Aplica ruff format e correções automáticas
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

typecheck:  ## mypy strict
	$(RUN) mypy

imports:  ## Contratos de arquitetura (import-linter)
	$(RUN) lint-imports

test:  ## Testes sem infraestrutura, com cobertura
	$(RUN) pytest -m "$(UNIT_MARKERS)" --cov --cov-report=term-missing

test-integration:  ## Testes que precisam de Oracle/RabbitMQ/Redis de teste
	$(RUN) pytest -m "oracle or rabbitmq or redis"

check: lint typecheck imports test  ## Tudo o que a Definição de Pronto exige

clean:  ## Remove caches
	rm -rf .mypy_cache .pytest_cache .ruff_cache .coverage htmlcov
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
