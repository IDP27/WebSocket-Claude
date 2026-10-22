# ADR-0022 — Gravação do lote do consumer no PostgreSQL (erro por linha)

- Status: Proposto (Fase 11)
- Data: 2026-10-21
- Altera: ADR-0009 (só o mecanismo; as garantias ficam iguais) e ADR-0013 §5. Depende do ADR-0021.

## Contexto

No Oracle, o lote do consumer grava os eventos brutos com `executemany(batcherrors=True)`. O driver aplica todas as linhas possíveis e devolve o erro de cada linha rejeitada. Com isso, o ADR-0009 separa:

- **duplicado:** ORA-00001 em `ohip_event_raw_uq_evt` ou `ohip_event_raw_uq_off`. É ignorado, com métrica, e não gera outbox;
- **falta de recurso**, mesmo que apareça numa linha só: desfaz o lote inteiro como banco indisponível. Nunca vira DLQ;
- **qualquer outro erro da linha:** vira item de DLQ `CONSUME` **na mesma transação**, e o lote segue.

O PostgreSQL não tem `batcherrors`. Um erro numa linha aborta a transação inteira. Para voltar a um ponto anterior é preciso `SAVEPOINT`, e cada savepoint custa um comando.

As garantias do ADR-0009 não podem mudar:
- nenhum evento some em silêncio;
- duplicado nunca gera outbox;
- falta de recurso nunca vira DLQ;
- o offset só avança pelo que foi commitado.

## Decisão

### 1. Dois caminhos dentro da mesma transação

```
SAVEPOINT eventos
caminho rápido:  executemany(INSERT ... ON CONFLICT ON CONSTRAINT ohip_event_raw_uq_evt
                             DO NOTHING RETURNING unique_event_id)
  ok             → linha com retorno = gravada; sem retorno = duplicado (uq_evt)
  erro 22xxx/23xxx (dado ou constraint) ou UnicodeEncodeError (surrogate, lado do cliente)
                 → ROLLBACK TO SAVEPOINT eventos → caminho lento
  erro de indisponibilidade → sobe (o lote inteiro é desfeito)
  outro erro     → BatchFailedError (bisseção do ADR-0009)

caminho lento, linha a linha, na ordem de chegada:
  SAVEPOINT linha; INSERT ... ON CONFLICT ON CONSTRAINT ohip_event_raw_uq_evt DO NOTHING
                   RETURNING unique_event_id; RELEASE SAVEPOINT linha
  erro → ROLLBACK TO SAVEPOINT linha e classifica:
    23505 em ohip_event_raw_uq_off       → duplicado (uq_off)
    indisponibilidade (ADR-0021 §5)      → sobe: desfaz o lote inteiro
    erro de dado (classes 22/23, inclusive 23505 na PK; UnicodeEncodeError) → falha da linha → DLQ CONSUME
    outro erro (ex.: 42xxx)                → BatchFailedError (bisseção e disjuntor, ADR-0009/0011)
```

No código, os savepoints são blocos `with conn.transaction():` aninhados dentro da transação do lote. As conexões têm `autocommit=True` (ADR-0021 §3).

O caminho rápido usa `executemany(..., returning=True)`, com `psycopg>=3.2`:
- o driver guarda um conjunto de resultados por linha, e um conflito gera um conjunto vazio;
- o adapter confere que o número de conjuntos é igual ao de linhas antes de classificar. Se não for, é `BatchFailedError` e nunca uma classificação deslocada.

Depois dos eventos, a ordem é a mesma de hoje (ADR-0013 §5):
1. outbox só dos gravados, na ordem de chegada;
2. DLQ;
3. item de retry, se houver;
4. offset (`rowcount` 0 → `UnknownChainError`);
5. barreira de epoch como **último comando**;
6. commit.

### 2. Por que o `ON CONFLICT` aponta só para `ohip_event_raw_uq_evt`

- `ON CONFLICT DO NOTHING` **sem alvo** engoliria conflito em **qualquer** constraint única, inclusive na PK (`id`). Um id repetido por bug viraria "duplicado" e o evento sumiria em silêncio. O ADR-0009 manda tratar conflito na PK como falha da linha.
- O PostgreSQL aceita um único alvo por `ON CONFLICT`. O alvo escolhido é `uq_evt`, porque é o duplicado comum: replay e reentrega têm o mesmo `uniqueEventId`.
- `uq_off` (mesmo chain, offset e `primaryKey` com outro `uniqueEventId`, D-6) é raro. Ele cai no caminho lento, onde o nome da constraint (`diag.constraint_name`) separa duplicado de falha.
- **Dois eventos com o mesmo `uniqueEventId` no mesmo lote:** o primeiro é gravado e o segundo não retorna linha, ou seja, duplicado. É o mesmo resultado do Oracle.

### 3. Custo

- **Caso normal:** sem dado ruim, que é o caso quase sempre, porque o domínio valida os mesmos limites da DDL antes do banco (ADR-0009). O custo é um `executemany` em pipeline e um `SAVEPOINT`.
- **Caminho lento:** só com dado que o domínio não pegou. Custa até 3 comandos por linha num lote de até 200 (`CONSUMER_BATCH_MAX_EVENTS`). É raro e limitado.
- O teste de carga mede o caso normal. Um cenário com 1 linha ruim a cada 1.000 mede o lento.

### 4. Ordem dos ids da outbox

- O `executemany` executa as linhas em ordem na mesma sessão, e `nextval('ohip_outbox_seq')` é chamado na ordem das linhas.
- Com `CACHE 1` (ADR-0021 §4), ids de sessões diferentes (consumer e API) também saem na ordem em que foram pedidos.

### 5. Retry de DLQ `CONSUME`, enricher e demais operações

- **Retry de DLQ:**
  - a mensagem é reprocessada no lote normal (§1);
  - `UPDATE` condicional (`resolution IS NULL`), igual ao de hoje;
  - `RETURNING chain_code` direto, sem variável de saída.
- **Enricher (ADR-0019):**
  - `INSERT ... ON CONFLICT (colunas da chave) DO UPDATE SET ... WHERE alvo.source_offset_num <= EXCLUDED.source_offset_num`;
  - linha não afetada = estado mais novo já gravado (`ohip_merge_skipped_total`);
  - a tabela de domínio (Q-1) precisa de um índice único nas colunas da chave, e o ADR dela declara isso.
- **Demais operações** (publisher, API, lease, status, expurgo): uma transação simples, sem erro por linha. Nenhuma mudança de desenho.

### 6. Testes obrigatórios (critério de aceitação)

1. **Os mesmos cenários de `tests/stores/`**, no fake e no PostgreSQL de teste. Incluem duplicado pelas duas constraints, falha por linha na DLQ, retry de DLQ e barreira de epoch.
2. **Lote misto no banco real:** gravado, duplicado `uq_evt`, duplicado `uq_off`, linha longa demais (`22001`), linha com NUL no payload (`22021`) e linha com surrogate isolado (`UnicodeEncodeError`).
   - Esperado: os dois duplicados em `BatchResult.duplicates` (o de `uq_off` também no log `evento_duplicado_por_offset`, com a constraint, por ser o caso raro da D-6); as três falhas na DLQ, com `raw_message` sem NUL nem surrogate; os outros eventos e a outbox gravados.
   - O offset e o `last_unique_event_id` seguem a regra do ADR-0009.
   - A métrica `ohip_duplicates_total` tem só o rótulo `chain_code` (nota no ADR-0009).
3. **Indisponibilidade no caminho lento:** uma linha com erro da classe `53` desfaz o lote inteiro (driver falso). Nada vai para a DLQ e o offset não avança.
4. **PK repetida:** vira falha da linha, nunca duplicado (driver falso e banco real).
5. **Ordem da outbox:** ids crescentes na ordem de chegada, com uma sessão da API inserindo reprocessamentos entre dois lotes.

## Consequências

- As garantias do ADR-0009 ficam iguais. Muda só o mecanismo, que agora depende de savepoints.
- O caminho lento, linha a linha, é mais lento que o `batcherrors`, mas só roda com dado inválido que passou pela validação do domínio.
- O ADR-0009 ganha uma nota apontando para este ADR. O texto "ORA-00001" vira "violação de unicidade (`23505`)".
