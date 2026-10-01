# ADR-0008 — Instância única por lease com epoch no Oracle

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

Dois consumers na mesma chain causam 4409 (lockout de ~2 min) e podem gravar fora de ordem. O PRD propõe uma trava no Redis com TTL de 30 s. Isso tem dois problemas:

1. **Split-brain**: se o processo pausar mais que o TTL (Oracle lento, GC, VM congelada), outro processo assume enquanto o primeiro ainda pode commitar.
2. **Ponto único de falha**: com a trava só no Redis, uma queda do Redis por mais de 30 s para a ingestão, embora o Redis "não seja fonte da verdade" (PRD) — risco para a meta de 99,5%.

O publisher tem o mesmo problema de instância única.

## Decisão

1. Tabela `OHIP_LEASE` no Oracle, uma linha por recurso (`consumer:<chain>`, `publisher`), com `epoch`, `owner` e `expires_at`. Todo horário vem do banco (`SYS_EXTRACT_UTC(SYSTIMESTAMP)`), sem depender do relógio das VMs.
2. **Aquisição**: `UPDATE ... SET epoch = epoch + 1, owner = :me, expires_at = agora + TTL WHERE lease_name = :n AND (expires_at < agora OR owner = :me)`. Uma linha atualizada = liderança, com o novo epoch guardado em memória. Zero linhas = outro dono; tenta de novo depois de `LEASE_TTL` + jitter. As linhas são **semeadas no provisionamento** (`sql/003_seed_leases.sql`), sem `MERGE` concorrente em tempo de execução; linha inexistente = erro de configuração (alerta).
3. **Renovação** a cada 10 s (TTL padrão 30 s) por uma **conexão de controle** própria de cada processo com lease (consumer e publisher), separada da conexão de escrita. Se não conseguir renovar antes de expirar, o processo entra em `DRAINING` e para de gravar.
4. **Barreira (fencing) no fim da transação**: toda transação de escrita (lote do consumer, marcação do publisher, replay aplicado, retry de DLQ `CONSUME`) termina, **imediatamente antes do commit**, com `UPDATE ohip_lease SET last_write_at = agora WHERE lease_name = :n AND epoch = :epoch`. Zero linhas → `ROLLBACK` e encerramento. Como o lock na linha do lease é pego só no fim, ele dura milissegundos: a renovação nunca fica presa atrás de uma escrita lenta (com o lock no começo, como na revisão anterior, uma escrita de 30 s bloquearia a renovação e provocaria failover espúrio).
   - Por que é seguro: se um novo dono incrementou o epoch e commitou antes, o `UPDATE` do zumbi não encontra a linha e ele desfaz tudo. Se o zumbi pegou o lock antes, a aquisição do novo dono espera o commit dele (milissegundos) e só então lê o offset, que já inclui essa escrita.
5. **Ao sair** (SIGTERM): `complete` → drena → grava `last_disconnect_at` → libera o lease com `UPDATE ohip_lease SET expires_at = agora WHERE lease_name = :n AND owner = :me AND epoch = :epoch`. Quem assumir respeita os 10 s a partir de `last_disconnect_at` (ADR-0007). **Se o processo está saindo porque perdeu o epoch, não libera nada**: o lease já é de outro dono.
6. A consulta de status do OHIP (ADR-0007, opcional) é uma barreira extra contra conexões fora do nosso controle (ex.: alguém testando com a mesma app key).
7. **Sem trava no Redis.** O Redis fica só com token, caches e métricas.

## Consequências

- Redis fora não para a ingestão nem a publicação (só perde atalhos e métricas).
- A barreira no fim serializa só o instante do commit, não a transação inteira.
- Alta disponibilidade ativo/passivo (Q-6) usa o mesmo mecanismo: a instância passiva fica em `ACQUIRING` e assume quando o lease expira.
- Custo: um `UPDATE` a cada 10 s por processo com lease (desprezível).

## Alternativas descartadas

- Trava só no Redis (PRD): split-brain e ponto único de falha.
- Trava no Redis + epoch no Oracle (versão 1 deste ADR): mantinha o Redis como ponto único de falha sem ganho de segurança, já que o Oracle passa a ser a autoridade.
- `DBMS_LOCK`: exige grant de pacote e segura sessão; o lease com prazo é mais simples de operar e observar.
