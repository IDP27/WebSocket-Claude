# ohip-streaming

Consumidor de eventos do OPERA Cloud pela **OHIP Streaming API** (WebSocket + GraphQL). Grava cada evento no Oracle 11g sem perda nem duplicidade e publica no RabbitMQ via outbox para BI, integrações e n8n.

> **Status: Fases 0 a 3 concluídas** (concepção, esqueleto, domínio e casos de uso, adapter Oracle). Próxima: Fase 4 (token OAuth, lease e Redis). A validação do adapter num Oracle 11g real aguarda ambiente (Q-17). Veja [docs/PLAN.md](docs/PLAN.md).
>
> **Endpoints**: a API de controle (FastAPI, [docs/API.md](docs/API.md)) e o painel são as Fases 7 e 8 e ainda não existem em código. As APIs da Oracle que o projeto chama estão em [postman/](postman/README.md).

## Ambiente de desenvolvimento

Pré-requisitos: Python 3.11+ e [Poetry 2](https://python-poetry.org/docs/#installation) (recomendado: `pipx install poetry`).

```bash
poetry install          # cria .venv no projeto (poetry.toml) com as versões do poetry.lock
cp .env.example .env    # preencha; o .env não é versionado
make check              # ruff + mypy + import-linter + pytest com cobertura
```

Outros alvos: `make help`. Testes que precisam de Oracle/RabbitMQ/Redis: `make test-integration`. Sem Poetry instalado, os alvos usam as ferramentas da `.venv` já criada.

### VS Code

Abra `ohip-streaming.code-workspace` e instale as extensões recomendadas (aviso no canto da tela).

- **Terminal → Run Task**: `make check` (também em Cmd+Shift+B), testes, lint, mypy, camadas, integração com Oracle de teste, integridade das specs da Oracle e cobertura em HTML.
- **Testing** (ícone de frasco): a suíte sem infraestrutura. Os testes de Oracle real aparecem como pulados até existir schema de teste.
- **Run and Debug**: `pytest: arquivo atual` e `pytest: suíte sem infraestrutura`.
- **APIs da Oracle**: a extensão Postman importa `postman/ohip-streaming.postman_collection.json` e o environment-modelo (só sandbox; veja [postman/README.md](postman/README.md)).

**macOS — "No module named ohip_streaming" fora da raiz:** se o arquivo `.venv/lib/python3.*/site-packages/ohip_streaming.pth` ganhar a flag `hidden`, o Python 3.12+ o ignora. Corrija com `chflags nohidden .venv/lib/python3.*/site-packages/*.pth`. O `Makefile` já define `PYTHONPATH=src` para não depender disso.

## Documentação

| Documento | Conteúdo |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Arquitetura, protocolo OHIP, fluxo de dados, modelo de dados, cache, escalabilidade |
| [docs/API.md](docs/API.md) | Contrato da API de controle (FastAPI) |
| [docs/UI.md](docs/UI.md) | Painel operacional (Flask) e wireframes |
| [docs/PLAN.md](docs/PLAN.md) | Plano por fases e caminho crítico |
| [docs/OPEN_QUESTIONS.md](docs/OPEN_QUESTIONS.md) | Perguntas em aberto e divergências PRD × documentação |
| [docs/OHIP_APIS.md](docs/OHIP_APIS.md) | APIs da Oracle usadas (Streaming, OAuth, REST do enricher) conferidas contra as specs oficiais |
| [vendor/oracle-hospitality-api-docs/](vendor/oracle-hospitality-api-docs/PROVENANCE.md) | Cópia fixada das specs oficiais da Oracle (ADR-0012); atualizar com `scripts/sync_oracle_api_docs.sh` |
| [postman/](postman/README.md) | Coleção e environment-modelo do Postman (sandbox) |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operação (em construção) |
| [docs/adr/](docs/adr/) | Decisões de arquitetura |
| [.env.example](.env.example) | Todas as variáveis de configuração, por grupo |
| [sql/](sql/) | DDL Oracle 11g numerada (rascunho) |
| [CLAUDE.md](CLAUDE.md) | Regras para o Claude Code, papéis e comandos |
