---
description: Investigar um bug ou incidente em produção até a causa raiz, com teste que reproduz e correção pronta
argument-hint: <descrição do bug, uniqueEventId, log ou código de fechamento>
---

Modo **Depuração em produção** para: $ARGUMENTS

Roteiro:

1. Reúna evidências: logs (filtre por `unique_event_id`, `chain_code`, `subscription_id`), `/api/v1/status`, itens de DLQ, código de fechamento (ver ADR-0007). Se houver `uniqueEventId`, exporte o evento (`GET /api/v1/events/{id}/export`) e reproduza localmente (RF-13). Nunca use o OHIP ou o banco de produção para reproduzir.
2. Leia com cuidado o código envolvido, raciocinando passo a passo sobre o caminho que o evento ou a conexão percorreu.
3. Formule hipóteses e descarte-as com evidência. Ache a **causa raiz**, não o sintoma.
4. Escreva primeiro um teste que **falha** reproduzindo o bug (unitário, ou contra `tests/fakes/fake_ohip_server.py`).
5. Corrija de forma robusta, sem quebrar as garantias (transação única, offset após commit, dedup, ordem por chain, epoch). Rode a suíte completa.
6. Passe pelo `revisor` antes de apresentar.

Formato de saída obrigatório:

1. **O que o código faz** (no trecho envolvido)
2. **Qual é o problema** (sintoma observado)
3. **Por que falha** (causa raiz, com evidência)
4. **Casos de borda** relacionados
5. **Teste que reproduz** (falha antes, passa depois)
6. **Correção pronta para produção** (diff) + impacto operacional (precisa replay? reprocessar DLQ?)
