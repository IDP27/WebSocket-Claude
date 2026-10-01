# ohip-streaming

Consumidor de eventos do OPERA Cloud pela **OHIP Streaming API** (WebSocket + GraphQL). Grava cada evento no Oracle 11g sem perda nem duplicidade e publica no RabbitMQ via outbox para BI, integrações e n8n.

> **Status: Fase 0 aprovada; Fase 1 (esqueleto) em andamento.** Veja [docs/PLAN.md](docs/PLAN.md).

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
| [sql/](sql/) | DDL Oracle 11g numerada (rascunho) |
| [CLAUDE.md](CLAUDE.md) | Regras para o Claude Code, papéis e comandos |
