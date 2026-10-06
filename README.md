# ohip-streaming

Consumidor de eventos do OPERA Cloud pela **OHIP Streaming API** (WebSocket + GraphQL). Grava cada evento no Oracle 11g sem perda nem duplicidade e publica no RabbitMQ via outbox para BI, integrações e n8n.

> **Status: Fases 0 a 10 concluídas** (a Fase 10 trouxe status completo do consumer, expurgo, teste de carga, systemd, Nginx e alertas; ADR-0020). Refatorações do diagnóstico `/entender` concluídas (passos 1 e 3 a 7: esperas e backoff únicos, ports e fakes por contexto, sessão do consumer, montagem comum dos processos e painel em módulos). O código está pronto até onde dá sem respostas externas; o que falta está em [Pendências externas](#pendências-externas). Veja [docs/PLAN.md](docs/PLAN.md).
>
> **Processos**: `ohip-consumer` (WebSocket OHIP → Oracle), `ohip-publisher` (outbox → RabbitMQ), `ohip-enricher` (fila → normalização no Oracle; sem regras até a Q-1, [ADR-0019](docs/adr/0019-enricher.md)), `ohip-purge` (expurgo diário por retenção), `ohip-api` (API de controle, [docs/API.md](docs/API.md)) e `ohip-admin` (painel em `/admin`, [docs/UI.md](docs/UI.md)), scripts do `pyproject.toml`.
>
> **Endpoints**: a API de controle está em [docs/API.md](docs/API.md) (OpenAPI em `/docs` fora de produção); o painel fica em `/admin` atrás do Nginx (login no Nginx/SSO, Q-5). As APIs da Oracle que o projeto chama estão em [postman/](postman/README.md).

## Pendências externas

Nada abaixo depende de código novo: cada item espera uma resposta ou um ambiente. Os pacotes em [docs/pendencias/](docs/pendencias/) estão prontos para enviar.

| Pendência | Quem resolve | O que destrava | Pacote |
| --- | --- | --- | --- |
| **Q-1**: quais eventos e hotéis entram (e a Q-2: buscar o recurso completo na REST?) | Negócio | Fase 9b: regras do enricher e tabelas de domínio (proibidas antes da resposta) | [Q-1-escopo-de-eventos.md](docs/pendencias/Q-1-escopo-de-eventos.md) |
| **Q-17**: schema Oracle 11g de teste descartável e VM x86_64 com Instant Client 19 | DBA da Aviva | Testes de integração do Oracle (Fase 3), planos de execução, expurgo e carga com banco real (Fase 10) | [Q-17-pedido-ao-dba.md](docs/pendencias/Q-17-pedido-ao-dba.md) |
| **Sandbox OHIP**: streaming habilitado, app de desenvolvimento e eventos aprovados | Oracle (workshop B93152 ou SR) e dono do ambiente | Validação do consumer (Fase 5) e dos pontos D da documentação | [sandbox-ohip.md](docs/pendencias/sandbox-ohip.md) |
| **RabbitMQ e Redis de teste da empresa** | Infraestrutura | Repetir os testes de integração no ambiente da empresa. Já passaram em contêineres locais (Redis 7 e RabbitMQ 3, 12 de 12) | [RUNBOOK](docs/RUNBOOK.md) |
| **Q-5, Q-6, Q-7, Q-8**: login do painel, VMs, ferramenta de alertas, pico de eventos | Projeto e infraestrutura | Ajuste dos padrões de deploy; limites `# AJUSTAR` em [ohip-alerts.yml](deploy/prometheus/ohip-alerts.yml); teste de carga com o número real | [OPEN_QUESTIONS](docs/OPEN_QUESTIONS.md) |

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
| [docs/pendencias/](docs/pendencias/) | Pacotes prontos para destravar as pendências externas (Q-1, Q-17, sandbox) |
| [docs/OHIP_APIS.md](docs/OHIP_APIS.md) | APIs da Oracle usadas (Streaming, OAuth, REST do enricher) conferidas contra as specs oficiais |
| [vendor/oracle-hospitality-api-docs/](vendor/oracle-hospitality-api-docs/PROVENANCE.md) | Cópia fixada das specs oficiais da Oracle (ADR-0012); atualizar com `scripts/sync_oracle_api_docs.sh` |
| [postman/](postman/README.md) | Coleção e environment-modelo do Postman (sandbox) |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operação: instalação, processos, expurgo, carga, alertas |
| [deploy/](deploy/) | Units systemd, Nginx e regras de alerta (Prometheus) |
| [docs/adr/](docs/adr/) | Decisões de arquitetura |
| [.env.example](.env.example) | Todas as variáveis de configuração, por grupo |
| [sql/](sql/) | DDL Oracle 11g numerada (rascunho) |
| [CLAUDE.md](CLAUDE.md) | Regras para o Claude Code, papéis e comandos |
