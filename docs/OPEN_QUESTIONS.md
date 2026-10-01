# Perguntas em aberto

> Até a resposta, cada item é tratado como **configurável** com o padrão indicado. Atualizado a cada fase.
> Legenda: **Q** = decisão de negócio/projeto · **D** = confirmação na documentação Oracle ou no sandbox · **DV** = divergência entre o PRD e a documentação (aprovadas em 2026-10-01).

## Q — Negócio e projeto

| ID | Pergunta | Bloqueia | Padrão até a resposta |
| --- | --- | --- | --- |
| Q-1 | Quais eventos e hotéis entram na fase 1? | Fase 9 (normalização) e tabelas de domínio | Reservas (NEW/UPDATE/CANCEL RESERVATION, CHECK IN, CHECK OUT) em uma chain; filtro `hotelCode` vazio (todos os aprovados) |
| Q-2 | Qual o primeiro caso de uso de negócio? | Latência alvo, obrigatoriedade do enriquecimento | Enriquecimento desligado; p95 ≤ 5 s só até o Oracle |
| Q-3 | ~~Versão do Oracle~~ | — | **Respondida: Oracle 11g** |
| Q-4 | A empresa acessa o OHIP como **cliente OPERA** ou **parceira**? | Custo (parceiro paga US$ 10 por 100 mil eventos), escopo da app (app de cliente vale para **uma** chain) | Cliente; uma app key por chain |
| Q-5 | Papel do Flask ao lado do FastAPI e forma de login (Nginx básico, SSO, LDAP)? | Fase 8 | Painel interno via API; usuário/grupo vindos de headers do Nginx |
| Q-6 | Quantas VMs (homologação/produção) e se haverá ativo/passivo desde o início | Fase 10 (deploy) | 1 VM por ambiente, `Restart=always`; mecanismo de trava já pronto para passiva (ADR-0008) |
| Q-7 | Ferramenta de alertas (e-mail, Teams, Grafana, Zabbix)? | Fase 10 | `/metrics` Prometheus + regras documentadas no RUNBOOK |
| Q-8 | Pico estimado de eventos por dia (e por minuto no fechamento do dia)? | RNF-06, RNF-13 (teste de carga) | 200 mil/dia, pico 300/min por chain (hipótese a substituir) |
| Q-9 | LGPD: quais campos de perfil podem ser guardados, mascarar já no bruto ou só nas saídas (logs, API, fila), e por quanto tempo? | Fase 3 (gravação), Fase 6 (contrato da fila) | Bruto íntegro com acesso restrito; máscara em logs, API e fila para uma lista configurável de `elementName`; 90 dias |
| Q-10 | Tipo de autenticação do ambiente: **Client Credentials (OCIM, com `enterpriseId`)** ou **Resource Owner (usuário de integração)**? | Fase 4 | Client Credentials; os dois suportados por configuração |
| Q-11 | O Oracle é RAC? | DDL (`OHIP_OUTBOX_SEQ` com `ORDER`) | Instância única (`NOORDER`) |
| Q-12 | Redis: dedicado ou compartilhado? Com senha/ACL e TLS? Aceitável guardar o token OAuth nele? | Fase 4 | Redis interno com senha; token guardado com TTL até o `exp − margem`. Com a DV-12, o Redis deixa de ser crítico para a ingestão |
| Q-13 | Quem declara as filas do n8n e de outros consumidores (nós ou cada time)? Convenção de nomes? | Fase 6 | Nós declaramos os exchanges `ohip.events`, `ohip.events.unrouted` e `ohip.reprocess` e a fila `ohip.enricher`; cada time declara a sua fila e o binding em `ohip.events` (reprocessamentos não chegam a terceiros) |
| Q-14 | É necessária carga histórica antes de ligar o streaming? | Fora do MVP (Pentaho/REST) | Não |
| Q-15 | A empresa fará escritas no OPERA no futuro (supressão de eco com `x-externalSystem`)? | Fora do MVP | Não |
| Q-16 | Como os logs chegam hoje ao MongoDB do time (coletor, agente, script)? | Fase 10 (deploy) | Serviço escreve JSON em stdout/journald; um coletor externo envia ao MongoDB (ADR-0010) |

## D — Confirmar na documentação Oracle ou no sandbox

| ID | Ponto | Situação | Como o design tolera |
| --- | --- | --- | --- |
| D-1 | O `offset` no `subscribe` é **inclusivo** (reenvia o evento daquele offset) ou exclusivo? | Doc diz "a partir daquele offset"; não define | O offset salvo e o `from_offset` do replay são sempre o **último já processado**; se for inclusivo, o evento volta e a deduplicação descarta; se for exclusivo, nada se perde (ADR-0006, API.md) |
| D-2 | Escopo OAuth exato (`urn:opc:hgbu:ws:__myscopes__`, com um ou dois `_`) e headers do `POST /oauth/v1/tokens` (`x-app-key`, `enterpriseId`) | Fontes divergem na grafia | Escopo, caminho e headers configuráveis; `TODO(confirmar-doc)` no adapter |
| D-3 | Fuso horário do campo `timestamp` do evento (`"2021-06-03 16:45:48.000"`, sem fuso) | Não documentado | Gravado como veio em `event_ts`; fuso configurável (`OHIP_EVENT_TZ`, padrão UTC) para a métrica de atraso |
| D-4 | Formato da consulta `query { connection { id status } }` no graphql-transport-ws (vai num `subscribe` com `id` próprio? resposta `next` + `complete`?) e valores possíveis de `status` | Doc mostra só a query | Etapa opcional (`STATUS_CHECK_ENABLED`); validar no sandbox antes de ligar em produção |
| D-5 | O guia diz que o Streaming "não é limitado", mas que "requisições de entrada" têm limite de 12/min (rajada 100/min). Vale para tentativas de conexão? Status query? | Não está claro | Reconexão com mínimo de 10 s; status query com intervalo mínimo de 30 s e desligada por padrão |
| D-6 | O guia define duplicado como "mesmo `primaryKey` e `offset`". Um offset pode ser reutilizado (ex.: ambiente recriado)? | Ambíguo | `UNIQUE(uniqueEventId)` e `UNIQUE(chain, offset, primaryKey)`; descarte + métrica por constraint (ADR-0009) |
| D-7 | O `offset` chega como string ou número no `next`? (exemplos divergem) | Ambíguo | Aceita os dois, normaliza para string (ADR-0006) |
| D-8 | Tamanho máximo esperado de uma mensagem `next` (para `max_size` do `websockets`) | Não documentado | `WS_MAX_MESSAGE_BYTES` configurável, padrão 16 MiB; excedente → reconexão; repetido 3× no mesmo offset → `STOPPED` + alerta |
| D-9 | `hotelCode` aceita lista separada por vírgula? (o schema diz que sim; o guia mostra só um código) | Ambíguo | Config `OHIP_HOTEL_CODES` como lista; validar com 2 hotéis no sandbox |
| D-10 | Quais `error` o OHIP envia na assinatura (ex.: offset fora da retenção de 7 dias, `chainCode`/`hotelCode` inválido) e se algum deles exige parar em vez de reconectar | O guia não lista os erros do frame `error` | `error`/`complete` com o nosso `id` encerram a assinatura e disparam reconexão com backoff exponencial (ARCHITECTURE §3, §4.1); motivo logado. Validar no sandbox na Fase 5 e, se houver erro permanente, tratá-lo como 4403 (`STOPPED` + alerta) |

## DV — Divergências entre o PRD e a documentação

> **DV-1 a DV-14 aprovadas em 2026-10-01**, junto com a Fase 0. Valem como decisão de projeto sobre o PRD.

| ID | PRD | Documentação Oracle | Proposta |
| --- | --- | --- | --- |
| DV-1 | Filtro `hotelId` no `subscribe` | O input é **`hotelCode`** (lista separada por vírgula segundo o schema; a confirmar, D-9); `hotelId` é campo de saída e pode vir nulo | Config `OHIP_HOTEL_CODES`; `hotel_id` nulo aceito no Oracle |
| DV-2 | Token em `ohip:token:<ambiente>` | Token **é por chain/ambiente** e não é intercambiável | Chave `ohip:token:<ambiente>:<chain>` |
| DV-3 | 4409: "aguardar e tentar de novo com backoff" | Lockout de **~2 min + jitter** | 120 s + jitter (ADR-0007) |
| DV-4 | Renovação: "envia `complete`, desconecta" | **Não fechar do lado do cliente**; esperar o servidor fechar | Drena e espera o fechamento com timeout (ADR-0007) |
| DV-5 | "Sem `pong` dentro do tempo limite → caída" | `pong` pode ser **adiado em rajadas**; servidor tolera 180 s; `next` conta como prova de vida | Timeout dinâmico `max(180 s, 4×SRTT)` (ADR-0007) |
| DV-6 | Códigos 4401/4403/4409/4504 | Existem também **1000, 4406, 4408** | Tratados (ADR-0007) |
| DV-7 | `connection_init` "com o token" | Payload leva `Authorization: Bearer` **e `x-app-key` (app key em claro)** | Seguir a doc; app key tratada como segredo |
| DV-8 | Consumer ↔ API "pelo Oracle ou pelo Redis" (prompt: replay via Redis) | — | Pedidos de replay em tabela Oracle `OHIP_REPLAY_REQUEST` (auditoria e durabilidade) |
| DV-9 | Routing key `ohip.rsv.UPDATE_RESERVATION` | `moduleName` vem como `RESERVATION`/`PROFILE`; `eventName` tem espaços | Mapa configurável `moduleName → código` + `eventName` com `_` (ARCHITECTURE §6) |
| DV-10 | API do PRD | — | Adições: `GET /api/v1/events/{id}/export` (RF-13), `GET /api/v1/replay` (acompanhar pedidos) e `DELETE /api/v1/replay/{id}` (cancelar pedido pendente) |
| DV-11 | Retenção do OHIP não citada | **7 dias**; conectar ao menos a cada hora | Alerta de conexão caída bem antes de 7 dias; aviso na API para replay > 7 dias |
| DV-12 | Trava de consumidor único no **Redis** (`ohip:lock:consumer:<chain>`, "depende do Redis por design") | — | **Lease com prazo e epoch no Oracle** (`OHIP_LEASE`) para consumer e publisher; nada de trava no Redis. Elimina split-brain e tira o Redis do caminho crítico (ADR-0008) |
| DV-13 | RF-03: "eventos via config" | Os eventos são escolhidos e aprovados no **Developer Portal**; o `subscribe` não filtra por evento | Filtro local opcional `OHIP_EVENT_ALLOWLIST` (vazio = todos): eventos fora da lista são gravados no bruto como `IGNORED`, avançam o offset e não vão para a outbox |
| DV-14 | Duplicado = mesmo `uniqueEventId` | Guia: duplicado = mesmo `primaryKey` e `offset` | As duas regras como `UNIQUE` no Oracle (ADR-0009) |
