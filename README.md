# ohip-streaming

Consumidor de eventos do OPERA Cloud pela **OHIP Streaming API** (WebSocket + GraphQL). Grava cada evento no Oracle 11g sem perda nem duplicidade e publica no RabbitMQ via outbox para BI, integrações e n8n.

> **Status: Fases 0 a 10 concluídas** (a Fase 10 trouxe status completo do consumer, expurgo, teste de carga, systemd, Nginx e alertas; ADR-0020). Refatorações do diagnóstico `/entender` concluídas (passos 1 e 3 a 7: esperas e backoff únicos, ports e fakes por contexto, sessão do consumer, montagem comum dos processos e painel em módulos). Próximo passo: regras do enricher após a Q-1. Pendentes: respostas da Q-1/Q-2 (regras do enricher), Oracle 11g de teste (Q-17), sandbox OHIP e RabbitMQ de teste. Veja [docs/PLAN.md](docs/PLAN.md).
>
> **Processos**: `ohip-consumer` (WebSocket OHIP → Oracle), `ohip-publisher` (outbox → RabbitMQ), `ohip-enricher` (fila → normalização no Oracle; sem regras até a Q-1, [ADR-0019](docs/adr/0019-enricher.md)), `ohip-purge` (expurgo diário por retenção), `ohip-api` (API de controle, [docs/API.md](docs/API.md)) e `ohip-admin` (painel em `/admin`, [docs/UI.md](docs/UI.md)), scripts do `pyproject.toml`.
>
> **Endpoints**: a API de controle está em [docs/API.md](docs/API.md) (OpenAPI em `/docs` fora de produção); o painel fica em `/admin` atrás do Nginx (login no Nginx/SSO, Q-5). As APIs da Oracle que o projeto chama estão em [postman/](postman/README.md).

## Ambiente de desenvolvimento

Pré-requisitos: Python 3.11+ e [Poetry 2](https://python-poetry.org/docs/#installation) (recomendado: `pipx install poetry`).

```bash
poetry install          # cria .venv no projeto (poetry.toml) com as versões do poetry.lock
cp .env.example .env    # preencha; o .env não é versionado
make check              # ruff + mypy + import-linter + pytest com cobertura
```

Outros alvos: `make help`. Testes que precisam de Oracle/RabbitMQ/Redis: `make test-integration`. Teste de carga do consumer: `make test-load`. Deploy (systemd, Nginx, alertas): [deploy/](deploy/) e [docs/RUNBOOK.md](docs/RUNBOOK.md). Sem Poetry instalado, os alvos usam as ferramentas da `.venv` já criada.

### VS Code

Abra `ohip-streaming.code-workspace` e instale as extensões recomendadas (aviso no canto da tela).

- **Terminal → Run Task**: `make check` (também em Cmd+Shift+B), testes, lint, mypy, camadas, integração com Oracle de teste, integridade das specs da Oracle e cobertura em HTML.
- **Testing** (ícone de frasco): a suíte sem infraestrutura. Os testes de Oracle real aparecem como pulados até existir schema de teste.
- **Run and Debug**: `pytest: arquivo atual`, `pytest: suíte sem infraestrutura`, `ohip-consumer (sandbox)`, `ohip-publisher (RabbitMQ de teste)`, `ohip-enricher (Oracle/RabbitMQ/Redis de teste)`, `ohip-purge (dry run, Oracle de teste)`, `ohip-api (Oracle/Redis de teste)` e `ohip-admin (painel, API local)`. Os processos leem o `.env` da raiz (copie do `.env.example`; o arquivo fica fora do git) e só devem apontar para sandbox OHIP e infraestrutura de teste. O painel precisa da API rodando e, sem Nginx, do header `X-Forwarded-User` em cada requisição (extensão do navegador ou `curl`).
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
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operação: instalação, processos, expurgo, carga, alertas |
| [deploy/](deploy/) | Units systemd, Nginx e regras de alerta (Prometheus) |
| [docs/adr/](docs/adr/) | Decisões de arquitetura |
| [.env.example](.env.example) | Todas as variáveis de configuração, por grupo |
| [sql/](sql/) | DDL Oracle 11g numerada (rascunho) |
| [CLAUDE.md](CLAUDE.md) | Regras para o Claude Code, papéis e comandos |
