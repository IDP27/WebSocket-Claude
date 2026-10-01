# ohip-streaming

Consumidor de eventos do OPERA Cloud pela **OHIP Streaming API** (WebSocket + GraphQL). Grava cada evento no Oracle 11g sem perda nem duplicidade e publica no RabbitMQ via outbox para BI, integrações e n8n.

> **Status: Fase 1 (esqueleto) em revisão.** Veja [docs/PLAN.md](docs/PLAN.md).

## Ambiente de desenvolvimento

Pré-requisitos: Python 3.11+ e [Poetry 2](https://python-poetry.org/docs/#installation) (recomendado: `pipx install poetry`).

```bash
poetry install          # cria .venv no projeto (poetry.toml) com as versões do poetry.lock
cp .env.example .env    # preencha; o .env não é versionado
make check              # ruff + mypy + import-linter + pytest com cobertura
```

Outros alvos: `make help`. Testes que precisam de Oracle/RabbitMQ/Redis: `make test-integration`.

**macOS — "No module named ohip_streaming" fora da raiz:** se o arquivo `.venv/lib/python3.*/site-packages/ohip_streaming.pth` ganhar a flag `hidden`, o Python 3.12+ o ignora. Corrija com `chflags nohidden .venv/lib/python3.*/site-packages/*.pth`. O `Makefile` já define `PYTHONPATH=src` para não depender disso.

## Documentação

| Documento | Conteúdo |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Arquitetura, protocolo OHIP, fluxo de dados, modelo de dados, cache, escalabilidade |
| [docs/API.md](docs/API.md) | Contrato da API de controle (FastAPI) |
| [docs/UI.md](docs/UI.md) | Painel operacional (Flask) e wireframes |
| [docs/PLAN.md](docs/PLAN.md) | Plano por fases e caminho crítico |
| [docs/OPEN_QUESTIONS.md](docs/OPEN_QUESTIONS.md) | Perguntas em aberto e divergências PRD × documentação |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operação (em construção) |
| [docs/adr/](docs/adr/) | Decisões de arquitetura |
| [.env.example](.env.example) | Todas as variáveis de configuração, por grupo |
| [sql/](sql/) | DDL Oracle 11g numerada (rascunho) |
| [CLAUDE.md](CLAUDE.md) | Regras para o Claude Code, papéis e comandos |
