# ADR-0009 — Deduplicação e tratamento de erros por linha no lote

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

O OHIP entrega "pelo menos uma vez" e replays reenviam eventos. O guia de troubleshooting define duplicado como "mesmo `primaryKey` e `offset`" e manda processar o primeiro. O PRD propõe `ohip:seen:<uniqueEventId>` no Redis, mas não diz **quando** a chave é gravada (antes do commit, uma falha perderia o evento). O insert em lote com `batcherrors` devolve erros por linha que não são só duplicidade.

## Decisão

- **Duas garantias no Oracle** em `OHIP_EVENT_RAW`:
  - `UNIQUE(unique_event_id)`;
  - `UNIQUE(chain_code, offset_value, primary_key)` — definição do guia Oracle.
- **Insert em lote** com `executemany(..., batcherrors=True)`. Para cada erro por linha:
  - ORA-00001 em `ohip_event_raw_uq_evt` ou `ohip_event_raw_uq_off` → **duplicado**: ignorado, métrica `ohip_duplicates_total{constraint}`, log com os dois ids. Não gera outbox.
  - **Qualquer outro erro** (ORA-12899, ORA-01400 etc.) → item de DLQ `CONSUME` com a mensagem bruta, **na mesma transação**. O evento nunca some em silêncio.
- **Lote que falha inteiro** de forma repetida com o Oracle respondendo: bisseção **em ordem**. Commita o prefixo, isola a mensagem culpada na DLQ `CONSUME` e segue com o sufixo; o offset só avança pelo que foi commitado.
- **Validação no domínio** com os mesmos limites da DDL (tamanhos e obrigatórios), para que a maior parte das mensagens ruins vá para a DLQ antes do banco.
- O offset salvo é o da **última mensagem do lote com offset válido**, na ordem de chegada (todas foram tratadas: gravada, duplicada ou DLQ por outro motivo). Se nenhuma tiver offset válido, `OHIP_OFFSET` não muda.
- **Atalho no Redis**: `ohip:seen:<uniqueEventId>` (TTL 24 h) é gravado **somente depois do commit**. Antes de montar o lote, o consumer filtra os ids já vistos. Falha do Redis não afeta a correção.
- Consumidores da fila (enricher, n8n) deduplicam por `message_id`; o enricher não deduplica reprocessamentos (exchange `ohip.reprocess`), marca o id como visto só após sucesso e usa `MERGE` condicional por offset com `<=` (ARCHITECTURE §4.3).

## Consequências

- Nenhum caminho de descarte depende só do Redis, e nenhuma linha rejeitada pelo banco some sem DLQ.
- Se o OHIP reutilizar um offset (ex.: ambiente recriado), eventos legítimos com o mesmo `(chain, offset, primaryKey)` seriam tratados como duplicados. É improvável, gera métrica e está em D-6.
