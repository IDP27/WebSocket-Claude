# ADR-0019 — Enricher: esqueleto sem regras (Fase 9)

- Status: Proposto (Fase 9)
- Data: 2026-10-05

## Contexto

O fluxo do enricher está em ARCHITECTURE §4.3 e o caso de uso `EnrichEvent` existe desde a Fase 2. A **Q-1** (quais eventos e hotéis entram) e a **Q-2** (primeiro caso de uso; padrão: enriquecimento desligado) seguem sem resposta. O CLAUDE.md proíbe criar tabelas de domínio ou regras de evento antes da Q-1. Esta fase entrega o processo completo **sem regras**: todo evento termina `UNMAPPED`, e a infraestrutura (fila, Oracle, REST, cache, rate limit, DLQ) fica pronta e testada para as regras entrarem depois da Q-1.

## Decisão

1. **Sem regras e sem tabelas de domínio.** O entrypoint monta `RuleRegistry(())` com `TODO(Q-1)`. As regras de teste existem só em `tests/`.
2. **Fila** (`adapters/rabbitmq/consumer.py`, `aio-pika`):
   - declara a fila `ohip.enricher` igual ao publisher (durável, sem argumentos, ADR-0016) e liga em `ohip.events` as routing keys de `ENRICHER_BINDINGS` (padrão vazio: sem regras, só os reprocessamentos chegam, pelo fanout `ohip.reprocess`). O exchange é conferido passivamente: quem declara é o publisher;
   - `prefetch` = `ENRICHER_PREFETCH`; uma mensagem por vez por processo; escala com N processos (ARCHITECTURE §8);
   - `is_reprocess` = a mensagem veio do exchange `ohip.reprocess` (nunca deduplicada, §4.4);
   - corpo que não é JSON ou sem `message_id`: log de erro e `ack` (não há como reprocessar; a fonte da verdade é o Oracle).
3. **Falhas** (`application/use_cases/enricher_service.py`, `EnricherService.handle`):
   - **transitória** (`StoreUnavailableError`, `ResourceUnavailableError`: REST fora, 5xx, 429, 401 repetido, rede): não conta tentativa; espera com backoff exponencial (`ENRICHER_TRANSIENT_BACKOFF_*`) com a mensagem ainda sem `ack` e devolve à fila (`nack requeue`);
   - **da mensagem** (regra falhou, REST 4xx, `StoreOperationError`, corrida de `MERGE` com ORA-00001): até `ENRICHER_MAX_ATTEMPTS` tentativas no próprio processo, com `ENRICHER_RETRY_DELAY_S`; esgotou → DLQ `NORMALIZE` (sem recurso REST) ou `ENRICH` (com recurso) e `processing_status = FAILED` na mesma transação, `message_id` marcado e `ack`. O retry pela API (Fase 7) reenvia pelo `ohip.reprocess`;
   - **disjuntor de falha sistêmica**: `ENRICHER_SYSTEMIC_FAILURE_THRESHOLD` (padrão 5) mensagens seguidas esgotando as tentativas com a mesma assinatura (estágio + classe da causa), sem sucesso no meio, abrem o disjuntor: alerta crítico `enricher_falha_sistemica` e, enquanto durar, essas mensagens são tratadas como transitórias (backoff e `REQUEUE`), não vão para a DLQ. Motivo: 403 da REST por falta de assinatura, `ENRICHER_RESOURCE_PATHS` errado, tabela sem grant (ORA-00942/01031) ou regra com bug afetam todas as mensagens e, sem o disjuntor, esvaziariam a fila na DLQ marcando tudo `FAILED` (o mesmo raciocínio do disjuntor do consumer, ADR-0011). Qualquer sucesso que não seja duplicado fecha o disjuntor. As mensagens anteriores ao limite ficam na DLQ (retry pela API). Limite aceito: com várias regras, a falha restrita a uma, intercalada com sucessos de outras, não abre o disjuntor;
   - **erro inesperado** (exceção que escapa do tratamento, bug nosso): alerta crítico `enricher_erro_inesperado`, backoff e `REQUEUE`. O processo não cai: ao reiniciar receberia a mesma mensagem e cairia de novo (laço de reinício);
   - **texto do erro**: log e `ohip_dlq.error_message` (que a API e o painel mostram) levam só o texto de `ApplicationError` (nossos erros, escritos sem dados pessoais). De qualquer outra exceção (ex.: `KeyError` com o valor de um campo do evento) fica só a classe e `detalhe omitido (pode conter dados pessoais)`; o log de erro inesperado também não leva traceback, só a classe e o local. Contrato das regras: para recusar um evento, `NormalizationRule.build` levanta `RuleError` com texto sem dados pessoais.
4. **Oracle** (`adapters/oracle/enrichment_store.py`), `PooledSession` com executor:
   - `apply`: numa transação, um `MERGE` por `DomainWrite` e o `UPDATE` do `processing_status`. Tabela e colunas vêm das regras (código, não entrada externa), mas são validadas (`^[A-Za-z][A-Za-z0-9_]{0,29}$`, limite do 11g) antes de entrar no SQL; valores sempre por bind. `WHEN MATCHED THEN UPDATE ... WHERE t.source_offset_num <= :source_offset_num`; linha não atualizada pela condição conta como ignorada (`ohip_merge_skipped_total`);
   - binds numerados (`:k1`, `:v1`…), não derivados do nome da coluna: coluna de 29–30 caracteres geraria bind acima do limite do 11g (ORA-00972);
   - sem lease nem barreira: N enrichers em paralelo são seguros pelo `MERGE` condicional (§4.3) **desde que a tabela de domínio tenha `UNIQUE` na chave natural** (as colunas do `ON`). Dois `MERGE` simultâneos da mesma entidade nova podem ambos cair no `WHEN NOT MATCHED`: com o `UNIQUE`, o segundo recebe ORA-00001 e a nova tentativa vira `UPDATE`; sem ele, ficam duas linhas em silêncio. O `UNIQUE` é requisito do ADR de cada tabela de domínio (após a Q-1);
   - chave com valor nulo é recusada pelo adapter (`t.col = NULL` nunca casa: o `MERGE` inseriria uma linha nova a cada evento). Hotel ausente (perfil da chain) vira `'#CHAIN'` na regra; as colunas da chave são `NOT NULL`;
   - `add_dlq(raw_event_id, stage, error_class, error)`: `INSERT ... SELECT` do bruto (chain, uid, offset) e `FAILED` no bruto, na mesma transação.
5. **REST do OHIP** (`adapters/ohip_rest/resources.py`), só com `ENRICHER_REST_ENABLED=true` (padrão `false`, Q-2):
   - `ResourceFetcher.fetch` passa a receber o `chain_code` (o token é por chain, DV-2); o enricher só tem credenciais da `OHIP_CHAIN_CODE`. Evento de outra chain que precise de recurso → falha da mensagem;
   - caminho por `moduleName` em `ENRICHER_RESOURCE_PATHS` (padrão: `RESERVATION` → `getReservation`, `PROFILE` → `getProfile`, docs/OHIP_APIS.md §4), com `{hotelId}` e `{primaryKey}` escapados;
   - headers `Authorization`, `x-app-key`, `x-hotelid` (obrigatório em todas as chamadas) e `x-request-id` (GUID). Evento sem `hotelId` → falha da mensagem (`TODO(confirmar-doc)`: perfil compartilhado sem hotel, D-13);
   - 200 → JSON; 204/404 → recurso ausente (`None`); 401 → token novo e uma nova tentativa, depois transitória; 403 e outros 4xx → falha da mensagem (repetido em todas, o disjuntor do §3 segura); 429 → transitória com `Retry-After` (pausa de todo o fetcher até o horário); 5xx/rede/timeout → transitória;
   - OAuth recusando as credenciais (ou 401 com token novo) segue transitória, porque a assinatura pode ser corrigida no Developer Portal sem reiniciar; mas `ENRICHER_REST_AUTH_REJECTED_ALERT` (padrão 5) recusas seguidas geram alerta crítico `ohip_rest_credenciais_recusadas` e a métrica `ohip_rest_auth_rejected_total`. O backoff do serviço (até `ENRICHER_TRANSIENT_BACKOFF_MAX_S`) limita os pedidos de token. Diferente do consumer, o enricher não para: parado ou esperando, a fila acumula igual, e esperando ele volta sozinho;
   - **rate limit** do lado do cliente: balde de fichas (`ENRICHER_REST_RATE_PER_S`, `ENRICHER_REST_BURST`);
   - **cache** no Redis: `ohip:rest:<chain>:<modulo>:<hotel>:<primaryKey>` por `ENRICHER_REST_CACHE_TTL_S` (padrão 45 s), inclusive o "não encontrado". A chain entra na chave (corrige ARCHITECTURE §7: hotéis de chains diferentes podem ter o mesmo código). Redis fora = sem cache.
6. **Processo `ohip-enricher`** (`entrypoints/enricher.py`): Oracle, Redis (dedup `ohip:processed:*` por `ENRICHER_DEDUP_TTL_S`, cache, métricas), RabbitMQ e, se habilitado, o cliente REST com token. Broker fora → reconecta com backoff. SIGTERM/SIGINT: termina a mensagem em curso e sai; mensagens sem `ack` voltam para a fila.

## Consequências

- Até a Q-1, ligar o enricher só marca eventos como `UNMAPPED`. Para medir o volume por `eventName` (métrica `ohip_events_unmapped_total`) é preciso `ENRICHER_BINDINGS=ohip.#`; com o padrão vazio só chegam reprocessamentos e a métrica fica zerada.
- O enricher só cria bindings, nunca remove: tirar uma routing key de `ENRICHER_BINDINGS` exige remover a binding no broker à mão (RUNBOOK).
- Uma regra nova (após a Q-1) precisa de: DDL da tabela de domínio com `source_event_raw_id`, `source_offset_num` e `UNIQUE` na chave natural `NOT NULL` (ADR), a regra em `NormalizationRule`, as routing keys em `ENRICHER_BINDINGS` e, se usar REST, `ENRICHER_REST_ENABLED=true`.
- Nova divergência D-13 (perfil sem `hotelId` × `x-hotelid` obrigatório).
