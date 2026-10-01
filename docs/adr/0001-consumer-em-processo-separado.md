# ADR-0001 — Consumer WebSocket em processo separado, um por chain

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

O OHIP aceita **um** assinante por (appKey, chainCode, gateway); um segundo recebe 4409 e o lockout dura cerca de 2 minutos. Uvicorn e Gunicorn sobem vários workers; se o consumer rodasse dentro deles, cada worker abriria sua conexão. O n8n também foi considerado (PRD, "Por que não colocar o WebSocket no n8n").

## Decisão

- O consumer é um processo próprio, `ohip-consumer@<chain>.service` (template systemd), com **uma** conexão WebSocket.
- Uma instância por chain. Chains diferentes rodam em paralelo (mesma app key só se a app for de parceiro; app de cliente é por chain).
- A conexão com a API/painel é indireta: o consumer grava estado em `OHIP_CONSUMER_STATUS` e lê pedidos em `OHIP_REPLAY_REQUEST`.
- O WebSocket não fica no n8n; o n8n consome a fila.

## Consequências

- Reinício e logs independentes da API; `Restart=always`.
- Precisamos de uma trava de consumidor único entre instâncias (ADR-0008).
- Comandos da API ao consumer têm latência de até um ciclo de verificação (padrão 5 s).

## Alternativas descartadas

- Consumer como tarefa de startup do FastAPI com `--workers 1`: frágil (qualquer aumento de workers quebra a regra) e mistura ciclos de vida.
- WebSocket no n8n: sem controle de transação, heartbeat e testes (PRD).
