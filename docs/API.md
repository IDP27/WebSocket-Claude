# API de controle — `ohip-api` (FastAPI)

> Status: **Contrato aprovado na Fase 0 (2026-10-01)**. Implementação na Fase 7. O OpenAPI gerado pelo FastAPI passa a ser a referência executável; este documento registra as decisões.

## Convenções

- Base: `/api/v1`, atrás do Nginx (TLS). `/health`, `/ready` e `/metrics` ficam na raiz e só são expostos na rede interna (Nginx bloqueia de fora).
- Autenticação: `Authorization: Bearer <token de serviço>`. Tokens configurados por ambiente como **hash SHA-256** (nunca em texto puro), cada um com perfil `read` ou `admin`.
- Auditoria: ações `admin` exigem o header `X-Actor` (usuário final repassado pelo `ohip-admin`, vindo do SSO/Nginx). Gravado em `requested_by` / `resolved_by`. O `ohip-admin` usa o token `read` ou `admin` conforme o grupo do usuário (ADR-0004).
- Rotas que tocam o Oracle são `def` (síncronas, threadpool do FastAPI) — ADR-0003.
- Paginação no banco com `ROWNUM` (11g): `?limit=` (padrão 50, máx. 200) e `?cursor=` (último `id` visto; paginação por chave, sem `OFFSET`).
- Datas em ISO-8601 UTC. Erros no formato:

```json
{"error": {"code": "REPLAY_OFFSET_INVALID", "message": "offset deve casar com ^[0-9]+$", "request_id": "..."}}
```

- `detail` de eventos é devolvido **mascarado** conforme a política LGPD, **inclusive no export**; sem máscara só para `admin` com `?unmasked=true` (auditado em log). Decisão final em Q-9.
- **Nenhuma rota publica na fila diretamente**: reprocessamentos e retries inserem linhas em `OHIP_OUTBOX` (ADR-0002).

## Endpoints

| Método | Rota | Perfil | Descrição |
| --- | --- | --- | --- |
| GET | `/health` | interno | Processo de pé (liveness). Não toca dependências. |
| GET | `/ready` | interno | Oracle, Redis e RabbitMQ acessíveis (readiness). 503 se algum falhar, com detalhe por dependência. |
| GET | `/metrics` | interno | Formato Prometheus (lista em ARCHITECTURE §10). Cache de 5 s. |
| GET | `/api/v1/status` | read | Estado por chain: `state`, último offset, `last_message_at`, atraso, reconexões, último código de fechamento, pendentes na outbox, DLQ aberta. |
| GET | `/api/v1/events` | read | Filtros: `chain_code`, `hotel_id`, `event_name`, `module_name`, `primary_key`, `from`, `to`, `processing_status`. |
| GET | `/api/v1/events/{unique_event_id}` | read | Evento bruto + status de processamento + linha da outbox + itens de DLQ ligados. |
| GET | `/api/v1/events/{unique_event_id}/export` | admin | **Adição ao PRD (RF-13):** JSON com payload (mascarado por padrão) e contexto para reprodução local. |
| POST | `/api/v1/events/{unique_event_id}/reprocess` | admin | Insere na outbox uma mensagem para o exchange `ohip.reprocess` (só o enricher recebe). O enricher não deduplica reprocessamentos e o `MERGE` usa `<=`, então reaplica a regra (idempotente). 202 Accepted. |
| POST | `/api/v1/replay` | admin | Pede replay de uma chain a partir de um offset. 202 Accepted. |
| GET | `/api/v1/replay` | read | **Adição ao PRD:** lista pedidos de replay e seu status (o painel precisa acompanhar o pedido). |
| DELETE | `/api/v1/replay/{id}` | admin | **Adição ao PRD:** cancela um pedido ainda `PENDING` (`CANCELLED`). |
| GET | `/api/v1/outbox` | read | Filtros: `status` (`PENDING`/`FAILED`), `chain_code`. Inclui idade da mais antiga. |
| GET | `/api/v1/dlq` | read | Filtros: `stage`, `chain_code`, `resolved` (bool). |
| POST | `/api/v1/dlq/{id}/retry` | admin | Reprocessa o item conforme o estágio. 202 Accepted. |

## Contratos principais

### `GET /api/v1/status`

```json
{
  "generated_at": "2026-10-01T12:00:00Z",
  "chains": [
    {
      "chain_code": "CHAIN1",
      "state": "SUBSCRIBED",
      "instance_id": "vm1:1234:9f2c",
      "subscription_id": "2b0e...",
      "last_offset": "97863",
      "last_message_at": "2026-10-01T11:59:58Z",
      "lag_seconds_p95_5m": 1.4,
      "events_per_minute": 37,
      "reconnects_total": 14,
      "consecutive_failures": 0,
      "next_attempt_at": null,
      "last_close": {"code": 1000, "reason": "token refresh", "at": "2026-10-01T11:02:10Z"},
      "token_expires_at": "2026-10-01T12:57:00Z",
      "outbox": {"pending": 0, "failed": 0, "oldest_pending_seconds": null},
      "dlq_open": {"CONSUME": 0, "PUBLISH": 0, "NORMALIZE": 0, "ENRICH": 3}
    }
  ]
}
```

### `POST /api/v1/replay`

Requisição:

```json
{"chain_code": "CHAIN1", "from_offset": "97000", "reason": "Lacuna após incidente INC-123", "confirm": "CHAIN1"}
```

Regras:

- `from_offset` é o **último offset já processado antes da lacuna** (o painel mostra esse texto). Funciona se o OHIP tratar o offset como inclusivo ou exclusivo (D-1).
- `from_offset` casa com `^[0-9]+$` e tem até 20 caracteres.
- `from_offset` **não pode ser maior** que o último offset confirmado da chain (um replay "para frente" pularia eventos). Rejeita com 422 `REPLAY_FORWARD_NOT_ALLOWED`.
- `confirm` deve repetir o `chain_code` (segunda confirmação, além do modal do painel).
- No máximo um pedido `PENDING` por chain (índice único no Oracle); senão 409.
- Aviso na resposta se o offset for provavelmente mais antigo que a retenção de 7 dias do OHIP.

Como o consumer aplica o pedido (caso de uso `ApplyReplay`, ARCHITECTURE §4.6):

1. Encontra o pedido `PENDING` (verificação a cada 5 s pela conexão de controle).
2. `complete` → drena e commita normalmente o que chegar → espera o servidor fechar.
3. Com a conexão fechada, a fila interna vazia e o escritor ocioso, no **thread de escrita**, numa transação com barreira de epoch: pedido → `APPLIED` (`WHERE status = 'PENDING'`; se o rowcount for 0, o pedido foi cancelado e não há replay) e `OHIP_OFFSET.last_offset = from_offset`. Commit.
4. Espera ≥ 10 s e assina com `from_offset`.

A troca do offset acontece sem conexão aberta, então nenhum lote a sobrescreve. Crash antes do passo 3: o pedido continua `PENDING` e é aplicado antes do próximo `subscribe`. Eventos já gravados são ignorados pela deduplicação; o replay só preenche lacunas.

Resposta `202`:

```json
{"id": 12, "status": "PENDING", "chain_code": "CHAIN1", "from_offset": "97000"}
```

### `POST /api/v1/dlq/{id}/retry`

| Estágio | O que o retry faz |
| --- | --- |
| `CONSUME` | Marca `retry_requested_at`/`retry_requested_by`. O **consumer da chain** revalida `raw_message` e, se válido, grava evento + outbox na sua transação com barreira de epoch (dedup aplicado), sem mexer no offset. A API não grava eventos. |
| `PUBLISH` | Cria uma **linha nova** na outbox (cópia da mensagem, no fim da fila da chain). A linha antiga continua `FAILED`. |
| `NORMALIZE` / `ENRICH` | Insere na outbox uma mensagem para o exchange `ohip.reprocess`. |

`PUBLISH`, `NORMALIZE` e `ENRICH` marcam o item com `resolution=RETRIED`, `resolved_by=X-Actor` **na mesma transação** da linha nova na outbox, condicionada a `resolution IS NULL`: dois cliques não duplicam a mensagem (o segundo recebe 409). No `CONSUME`, quem resolve é o consumer depois de gravar; se a mensagem continuar inválida, o item fica aberto com o motivo novo e o retry pode ser pedido de novo.

`POST /events/{id}/reprocess` e o retry de `NORMALIZE`/`ENRICH` recusam evento `IGNORED` (fora da allowlist, DV-13) com 409.

## Códigos de erro

| HTTP | `code` | Quando |
| --- | --- | --- |
| 400 | `VALIDATION_ERROR` | Parâmetro inválido |
| 401 | `UNAUTHENTICATED` | Sem token ou token inválido |
| 403 | `FORBIDDEN` | Perfil `read` tentando ação `admin`, ou sem `X-Actor` |
| 404 | `NOT_FOUND` | Evento, item de DLQ ou chain inexistente |
| 409 | `REPLAY_ALREADY_PENDING` | Já há replay pendente na chain |
| 409 | `REPLAY_NOT_PENDING` | Cancelamento de pedido que não está `PENDING` |
| 409 | `INVALID_STATE` | Item de DLQ já resolvido ou com retry já pedido; item sem a referência necessária; evento `IGNORED` |
| 422 | `REPLAY_FORWARD_NOT_ALLOWED` | `from_offset` maior que o último offset confirmado |
| 503 | `DEPENDENCY_UNAVAILABLE` | Oracle indisponível |
