# Validação no sandbox do OHIP (checklist único)

> Pontos que a documentação da Oracle não fecha e que só o sandbox confirma (`docs/OPEN_QUESTIONS.md`, itens D). O código já tolera as duas respostas possíveis de cada um; a validação decide o padrão e tira o `TODO(confirmar-doc)`. **Só sandbox ou UAT, nunca a app key de produção.**

## 1. Pré-requisitos

1. Streaming habilitado no ambiente (workshop B93152 ou SR) e app registrada com eventos aprovados (PLAN, ações externas).
2. App key **de desenvolvimento**, separada da do consumer de UAT. Cada conexão disputa o consumidor único: duas ao mesmo tempo dão 4409 e lockout de ~2 min.
3. Credenciais só como *current value* no Postman ou variáveis de sessão (`postman/README.md`).
4. Uma pessoa com acesso ao OPERA do sandbox para gerar eventos (criar e alterar uma reserva de teste, sem dado pessoal real).

## 2. Parte A — com o cliente oficial (`graphiql.html`), sem Oracle

Abrir `vendor/oracle-hospitality-api-docs/graphql/streaming/graphiql.html` no navegador, com a app key **e um token OAuth do sandbox** (gerado pelo Postman, **1. OAuth → Token**; o cliente oficial pede os dois). Anotar cada resultado na tabela do §4.

| ID | Como validar | O que observar |
| --- | --- | --- |
| D-7 | Assinar `newEvent` e gerar um evento no OPERA | `metadata.offset` chega como string (o schema diz `String!`) |
| D-1 | Anotar o offset `N` de um evento. Fechar, esperar ≥ 10 s e assinar de novo com `offset: "N"` | O primeiro evento entregue é o `N` (inclusivo) ou o seguinte (exclusivo) |
| D-11 | Mesmos eventos | Quantos dígitos tem o offset; algum passa de 10? |
| D-9 | Assinar com `hotelCode: "H1,H2"` (dois hotéis do sandbox) | Chegam eventos dos dois? Algum erro com a lista? |
| D-10 | Assinar com `chainCode` inexistente; depois com um offset muito antigo (fora dos 7 dias) | Formato do frame `error` (mensagem, `extensions`) e se o servidor fecha a conexão, com qual código |
| D-4 | Rodar `query { connection { id status } }` no GraphiQL (o cliente oficial a envia num `subscribe` com `id` próprio, OHIP_APIS §3) | Resposta `next` + `complete`; valores de `status` |
| D-3 | Gerar um evento numa hora conhecida | Fuso do `timestamp` (UTC, hora do hotel ou do servidor?) |
| D-8 | Gerar uma reserva com muitas alterações de uma vez | Tamanho da maior mensagem `next` (bytes) |

## 3. Parte B — com o consumer do projeto

Precisa também do Oracle de teste (Q-17), de um RabbitMQ e de um Redis de teste. Rodar com `APP_ENVIRONMENT=desenvolvimento`, `OHIP_GATEWAY_URL` do sandbox e a app key de desenvolvimento.

| Ponto | Como validar | Esperado (ADR-0007) |
| --- | --- | --- |
| Conexão e eventos | Subir `ohip-consumer` e gerar eventos | `SUBSCRIBED` ("CONECTADO" no painel), eventos em `OHIP_EVENT_RAW`, offset avançando, mensagens no RabbitMQ |
| Heartbeat | Deixar 30 min sem eventos | Nenhuma reconexão; `last_pong_at` e RTT atualizando no painel |
| Renovação de token | Deixar passar o `exp` do token (ou reduzir `OHIP_TOKEN_REFRESH_MARGIN_S`) | `complete` → drenagem → servidor fecha → reconexão após ≥ 10 s com o último offset |
| 4409 | Assinar primeiro no `graphiql.html` com a **mesma** app key; depois subir (ou reiniciar) o consumer. Fechar o GraphiQL após alguns minutos | Consumer em `WAITING` com próxima tentativa em 120–150 s (120 s + jitter); volta sozinho depois que o GraphiQL fecha; o alerta `OhipLockout4409Repetido` (`deploy/prometheus/ohip-alerts.yml`) dispara com 3 ou mais 4409 em 30 min. Anotar também o que o GraphiQL recebe se for ele o segundo assinante (pelo guia, 4409 vai para o segundo: ARCHITECTURE §2) |
| Replay | Pedir replay pelo painel para um offset de minutos atrás | Eventos reentregues; duplicados descartados pelos `UNIQUE` (ADR-0009) |
| Parada | `systemctl stop` (ou SIGTERM) | `complete`, drenagem, estado `WAITING` com `last_disconnect_at` gravado e lease liberado (`STOPPED` é só para 4403/4406 e mensagem grande demais); o reinício respeita os 10 s |
| D-13 (se REST ligada) | Evento de perfil compartilhado (sem `hotelId`) | Hoje vai para a DLQ `ENRICH`; anotar qual `x-hotelid` a Oracle aceita |

## 4. Resultado

| ID | Data | Resultado | Ação no projeto |
| --- | --- | --- | --- |
| D-1 | | | Nenhuma se inclusivo (a dedup descarta); se exclusivo, registrar e manter |
| D-3 | | | Ajustar o padrão de `OHIP_EVENT_TIMEZONE` |
| D-4 | | | Se confirmado, permitir ligar `OHIP_STATUS_CHECK_ENABLED` |
| D-7 | | | — (já resolvida pelo schema) |
| D-8 | | | Ajustar `OHIP_WS_MAX_MESSAGE_BYTES` se necessário |
| D-9 | | | Manter ou restringir `OHIP_HOTEL_CODES` a um código |
| D-10 | | | Erro permanente → tratar como 4403 (`STOPPED` + alerta), com ADR |
| D-11 | | | Se o limite for 10, registrar o risco e o tratamento (ADR-0006) |
| D-13 | | | Decidir o `x-hotelid` dos perfis compartilhados |

Depois: atualizar `docs/OPEN_QUESTIONS.md`, remover os `TODO(confirmar-doc)` resolvidos e marcar a validação da Fase 5 no `docs/PLAN.md`.
