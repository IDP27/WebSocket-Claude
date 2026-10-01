# ADR-0005 — Arquitetura em camadas com regra de dependência verificada

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

O MVP precisa nascer pronto para crescer sem ser reconstruído (PRD, RNF-14). As dependências externas (Oracle 11g, RabbitMQ, OHIP) podem mudar.

## Decisão

- Camadas: `domain` → regras puras; `application` → casos de uso e ports (`typing.Protocol`); `adapters` → implementações; `entrypoints` → processos e interfaces.
- Regra: `entrypoints → application → domain`; `adapters → application/domain`. `domain` e `application` não importam `oracledb`, `websockets`, `fastapi`, `flask`, `aio_pika`, `redis`, `httpx`.
- Contratos no `.importlinter` (Fase 1):
  - `layers`: entrypoints > adapters > application > domain.
  - `forbidden`: domain/application → bibliotecas de infraestrutura.
  - `forbidden`: `entrypoints.admin` → `adapters.oracle`, `adapters.redis`, `oracledb`, `redis` (ADR-0004).
- A composição (injeção dos adapters) acontece só nos entrypoints.

## Consequências

- Casos de uso testáveis com fakes, sem infraestrutura.
- Um pouco mais de código (ports e fakes) desde o início.
