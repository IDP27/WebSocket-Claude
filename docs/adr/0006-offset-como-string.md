# ADR-0006 — Offset tratado como string

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

A documentação Oracle diz que o offset "é uma string, não um número" e deve ser enviado como string no `subscribe`. O schema GraphQL oficial (`oracle/hospitality-api-docs`, `StreamingGraphQLSchema.json`) valida `offset` com `^[0-9]+$` e tipo `StringWithLength20`. Um exemplo do guia mostra `"offset": 100` (número), então a forma no JSON pode variar.

## Decisão

- Tipo de domínio `Offset`: aceita string ou inteiro no JSON recebido, normaliza para string e valida `^[0-9]{1,20}$`. Fora do padrão → DLQ `CONSUME`.
- Oracle: `VARCHAR2(20)`. Enviado sempre como string no `subscribe`.
- O consumer **não decide o que gravar** comparando offsets (dedup pelas constraints do ADR-0009; ordem é a de chegada). Comparação numérica permitida só em: métricas de lag; detecção de retrocesso (log de aviso); **validação de replay** (não aceitar `from_offset` maior que o último offset confirmado, o que pularia eventos); e a **condição do `MERGE`** do enricher (`source_offset_num`), que só compara eventos da mesma chain.
- O offset salvo é o do último evento do lote commitado. Se o OHIP reenviar o evento desse offset no replay (inclusivo ou não — D-1), a deduplicação absorve.

## Consequências

- Nenhuma suposição sobre o formato numérico vaza para o protocolo.
- Ordenações por offset no SQL precisam de `LPAD(offset_value, 20, '0')` ou `TO_NUMBER`; usadas só em consultas operacionais.
