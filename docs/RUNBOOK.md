# Runbook — ohip-streaming

> Status: **esqueleto da Fase 0**. Procedimentos completos na Fase 10.

## Pré-requisitos (manuais, antes do primeiro deploy)

1. Ambiente OPERA Cloud ≥ 22.3.0.1 com **Streaming Enabled** no Developer Portal (cliente: workshop B93152 para a primeira chain UAT; demais via SR técnico).
2. Aplicação registrada no OHIP com os eventos da fase 1 **aprovados** pelo dono do ambiente. Uma app key separada por ambiente e outra para desenvolvimento.
3. `clientId`, `clientSecret`, app key, gateway URL e, se Client Credentials, `enterpriseId` — no cofre/ambiente, nunca no repositório.
4. Se o ambiente usar OCIM: vida do token OAuth ≥ 60 min.
5. VM Linux com saída `wss://` 443 para o gateway OHIP, **sem inspeção TLS**, idle timeout de NAT/firewall > 15 s.
6. Oracle Instant Client 19 instalado; schema e usuários conforme `sql/` (aplicado pelo DBA).
7. RabbitMQ e Redis acessíveis pela VM.

## Testes contra o Oracle de teste (Fase 3)

Os cenários de `tests/stores/` rodam no fake e, com estas variáveis, num Oracle 11g **de teste**:

1. O DBA cria um schema **descartável** e aplica `sql/001` a `003` (o `publisher` precisa existir em `OHIP_LEASE`). Os testes não rodam DDL; eles apagam e semeiam só as linhas das chains `ZZT1`/`ZZT2`.
2. Numa máquina x86_64 com Instant Client 19 (não existe para Mac arm64):

   ```bash
   export TEST_ORACLE_DSN=host:1521/servico TEST_ORACLE_USER=ohip_test TEST_ORACLE_PASSWORD='***'
   export TEST_ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_19_x TEST_ORACLE_DISPOSABLE_SCHEMA=sim
   make test-integration
   ```

   Sem `TEST_ORACLE_DISPOSABLE_SCHEMA=sim` os testes de Oracle são pulados. **Nunca** aponte para homologação ou produção.

## Hash da app key

```bash
echo -n "$OHIP_APP_KEY" | sha256sum | cut -d' ' -f1   # hex minúsculo
```

## Códigos de fechamento (resumo — ADR-0007)

| Código | O que fazer |
| --- | --- |
| 4401 | Automático (novo token). Se repetir: conferir credenciais e hash da app key. |
| 4403 | **Ação humana**: conferir chain, habilitação de streaming e aprovação dos eventos no Developer Portal. Depois `systemctl restart ohip-consumer@<chain>`. |
| 4406 | Bug no handshake (subprotocolo). Abrir incidente para o time. |
| 4409 | Automático (espera 2 min). Se persistir: procurar outro cliente usando a mesma app key (Postman, n8n, outra VM). |
| 4504 | Automático (15 s). Se persistir: status da Oracle. |

## Cuidados

- Nunca usar a app key de produção no Postman (ele não envia `ping` e derruba/disputa a conexão).
- Postman só com a coleção e o environment de `postman/` (sandbox, valores preenchidos só como *current value*, nunca commitados). Para explorar o Streaming no sandbox, use o `graphiql.html` oficial em `vendor/oracle-hospitality-api-docs/graphql/streaming/`.
- Atualizar as specs da Oracle só com `scripts/sync_oracle_api_docs.sh <commit>` e `make check` verde (ADR-0012).
- Replay só pela API/painel (`POST /api/v1/replay`); retenção do OHIP é de 7 dias.
- Expurgo, nesta ordem (chaves estrangeiras): `OHIP_DLQ` resolvida → `OHIP_OUTBOX` `SENT` → `OHIP_EVENT_RAW` sem referência em outbox ou DLQ (`NOT EXISTS`). Linhas `FAILED` e DLQ abertas seguram o bruto até serem resolvidas.
- Alerta `ohip_merge_skipped_total` alto + offset menor que o salvo: o OHIP pode ter reiniciado os offsets (ambiente recriado, D-6). Parar o enricher, confirmar com a Oracle e decidir com o time de dados se as tabelas de domínio devem ser reconstruídas a partir do bruto.
- Alerta `falha_sistemica_no_lote` (consumer parado em `WAITING`, reconectando sem avançar): duas mensagens seguidas foram recusadas pelo banco sem nenhum progresso entre elas (ADR-0011). Primeiro conferir o Oracle (espaço em tablespace/undo, alert log, objetos inválidos) e corrigir; o consumer volta sozinho na próxima reconexão. Se o banco estiver saudável e o erro for de uma mensagem específica (ver o log e o último item de DLQ `CONSUME` da chain), `systemctl restart ohip-consumer@<chain>`: cada restart isola **uma** mensagem na DLQ. Depois da correção, reprocessar os itens pela API (`POST /api/v1/dlq/{id}/retry`).
- Alerta `oauth_recusado` (400/401/403 no token): credencial, app key, escopo ou `enterpriseId` errados, ou assinatura ainda não aprovada no Developer Portal. O processo não insiste em seguida; corrija a configuração e reinicie.
- Alerta `lease_perdido`: outro processo assumiu (failover) ou o banco ficou sem responder além de `LEASE_TTL_S`. O processo para de gravar e sai; o systemd o reinicia, e ele espera o lease. Duas instâncias ativas ao mesmo tempo nunca gravam juntas (barreira de epoch).
- `LeaseNotProvisionedError`: falta a linha em `OHIP_LEASE` (`sql/003_seed_leases.sql` com o `chain_code`).
- Redis fora: ingestão e publicação continuam; perdem-se o token compartilhado (cada processo pede o seu), o dedup rápido e as métricas de contadores. Testes contra um Redis de teste: `TEST_REDIS_URL` + `TEST_REDIS_DISPOSABLE=sim` e `make test-integration`.
- Units systemd com `RestartSec=10`: reiniciar em menos de 10 s após uma desconexão provoca 4409 e lockout de ~2 min.
