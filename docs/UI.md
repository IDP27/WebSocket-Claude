# Painel operacional — `ohip-admin` (Flask)

> Status: **Proposta aprovada na Fase 0 (2026-10-01)**. Implementação na Fase 8.

## Princípios

- Renderização no servidor com Jinja2, **sem SPA**. Blocos de status se atualizam a cada 10–15 s com HTMX (`hx-get` + `hx-trigger="every 15s"`) trocando só o fragmento.
- **O Flask não acessa o Oracle nem o Redis**: consome apenas a `ohip-api`. Usa o token de serviço `read` ou `admin` conforme o grupo do usuário e repassa o usuário em `X-Actor` (ADR-0004).
- Autenticação do usuário no Nginx/SSO (Q-5). O Flask lê o usuário e o grupo de headers confiáveis definidos pelo Nginx (removidos se vierem do cliente). Ações de escrita só aparecem para o grupo admin, e o Flask confere o grupo no servidor em todo POST; a autorização final é da API.
- Sem JavaScript próprio além do HTMX; sem dados pessoais sem máscara na tela.

## Páginas

| Rota | Página | Conteúdo | Atualização |
| --- | --- | --- | --- |
| `/admin/` | Visão geral | Cartões por chain (estado, último offset, atraso, eventos/min, reconexões); outbox; DLQ aberta | Fragmentos a cada 15 s |
| `/admin/events` | Eventos | Busca com filtros e tabela paginada | Sob demanda |
| `/admin/events/<uid>` | Detalhe do evento | Cabeçalho, `detail` mascarado, outbox, DLQ ligada, botões Reprocessar e Exportar | Sob demanda |
| `/admin/dlq` | DLQ | Tabela por estágio; erro; botão Reprocessar | Fragmento a cada 15 s |
| `/admin/operations` | Operações | Formulário de replay por offset com confirmação dupla; histórico de pedidos | Fragmento a cada 15 s |

## Componentes (macros Jinja2)

- `layout.html`: cabeçalho com ambiente (HOMOLOGAÇÃO em amarelo, PRODUÇÃO em vermelho), usuário e navegação.
- `status_card(chain)`: estado com cor e texto (nunca só cor), métricas e "atualizado há N s".
- `paged_table(columns, rows, cursor)`: tabela com paginação por cursor.
- `confirm_modal(action, expected_text)`: modal que exige digitar o texto esperado (ex.: código da chain).
- `flash_messages()`.

## Wireframes

### Visão geral

```
┌──────────────────────────────────────────────────────────────────────────┐
│ OHIP Streaming · PRODUÇÃO                         usuario@empresa  [Sair]│
│ Visão geral | Eventos | DLQ | Operações                                  │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌─ CHAIN1 ───────────────── ● CONECTADO ─┐ ┌─ CHAIN2 ──── ● AGUARDANDO ─┐│
│ │ Último offset   97863                  │ │ Último offset   1200        ││
│ │ Última msg      há 2 s                 │ │ Última msg      há 4 min    ││
│ │ Atraso p95 5m   1,4 s                  │ │ Fechamento 4409 · falhas 2  ││
│ │ Eventos/min     37                     │ │ Próx. tentativa em 1m 40s   ││
│ │ Reconexões      14 · token expira 57m  │ │                             ││
│ └────────────────────────────────────────┘ └─────────────────────────────┘│
│ ┌─ Outbox ───────────────────┐ ┌─ DLQ aberta ──────────────────────────┐ │
│ │ Pendentes 0 · Falhas 0     │ │ CONSUME 0 · PUBLISH 0 · ENRICH 3      │ │
│ │ Mais antiga: —             │ │ [ver DLQ]                             │ │
│ └────────────────────────────┘ └───────────────────────────────────────┘ │
│                                                      atualizado há 3 s   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Eventos

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Chain [CHAIN1 ▾] Hotel [____] Evento [UPDATE RESERVATION ▾]              │
│ De [2026-10-01 00:00] Até [2026-10-01 23:59] Status [todos ▾] [Buscar]   │
├────────────┬────────┬──────────┬─────────────────────┬──────────┬───────┤
│ Recebido   │ Hotel  │ Offset   │ Evento              │ Chave    │Status │
├────────────┼────────┼──────────┼─────────────────────┼──────────┼───────┤
│ 11:59:58   │ HOTEL1 │ 97863    │ UPDATE RESERVATION  │ 1234567  │ENRICH.│
│ 11:59:41   │ HOTEL1 │ 97862    │ NEW PROFILE         │ 7654321  │UNMAP. │
├────────────┴────────┴──────────┴─────────────────────┴──────────┴───────┤
│                                                    [Próxima página ▸]   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Detalhe do evento

```
┌──────────────────────────────────────────────────────────────────────────┐
│ UPDATE RESERVATION · 5c00f504-3844-4a23-91af-feb150b06568                │
│ Chain CHAIN1 · Hotel HOTEL1 · Offset 97863 · Chave 1234567               │
│ Evento 11:59:57.900 · Recebido 11:59:58.812 · Gravado 11:59:58.990       │
│ Status ENRICHED · Outbox SENT (1 tentativa)                              │
├──────────────────────────────────────────────────────────────────────────┤
│ Elemento          │ Antes            │ Depois                            │
│ ARRIVAL DATE      │ 2026-10-10       │ 2026-10-11                        │
│ FIRST NAME        │ ***              │ ***                               │
├──────────────────────────────────────────────────────────────────────────┤
│ [Reprocessar]  [Exportar JSON]                       (somente admin)     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Operações (replay)

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Replay por offset                                                        │
│ Chain [CHAIN1 ▾]   Último offset bom antes da lacuna [_______] (≤ 97863) │
│ Motivo [______________________________________________]                  │
│ ⚠ O consumer fará uma reconexão controlada (~10–20 s sem eventos).       │
│   Eventos já gravados serão ignorados. Retenção do OHIP: 7 dias.         │
│                                                     [Pedir replay…]      │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌ Confirmar replay ───────────────────────────────────────┐              │
│ │ Digite CHAIN1 para confirmar: [______]                  │              │
│ │                           [Cancelar]  [Confirmar]       │              │
│ └─────────────────────────────────────────────────────────┘              │
├──────────────────────────────────────────────────────────────────────────┤
│ Histórico                                                                │
│ #12  CHAIN1  97000  APPLIED   joao@…   2026-10-01 10:02                  │
│ #13  CHAIN1  98100  PENDING   ana@…    2026-10-01 11:40   [Cancelar]     │
└──────────────────────────────────────────────────────────────────────────┘
```

### DLQ

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Estágio [todos ▾]  Chain [todas ▾]  [ ] mostrar resolvidos               │
├──────┬─────────┬────────┬──────────────────────────────┬────────┬───────┤
│ ID   │ Estágio │ Chain  │ Erro                         │ Tent.  │ Ação  │
├──────┼─────────┼────────┼──────────────────────────────┼────────┼───────┤
│ 31   │ ENRICH  │ CHAIN1 │ HTTP 429 após 5 tentativas   │ 5      │[Repr.]│
│ 30   │ CONSUME │ CHAIN1 │ uniqueEventId ausente        │ 1      │[Repr.]│
└──────┴─────────┴────────┴──────────────────────────────┴────────┴───────┘
```
