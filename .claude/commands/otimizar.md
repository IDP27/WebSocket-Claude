---
description: Otimizar desempenho a partir de medição (RNF-13 ou métrica estourada), com comparação antes/depois
argument-hint: <métrica, processo ou cenário de carga>
---

Modo **Otimização de desempenho** para: $ARGUMENTS

Use o subagente `otimizador`. Sem medição que mostre o estouro, pare e peça a medição.

Roteiro:

1. Meça primeiro: teste de carga em `tests/load/` contra o servidor simulado (10× o pico, Q-8), latência p50/p95/p99, eventos/s, memória RSS ao longo do tempo, CPU.
2. Perfile com `py-spy` e métricas do Oracle (tempo por transação, tamanho do lote).
3. Procure: gargalos, lógica ineficiente, I/O desnecessário ou em laço, bloqueio do event loop, consultas sem índice, renderização desnecessária no painel (fragmentos HTMX grandes, polling excessivo).
4. Mude uma coisa por vez, sem afrouxar garantias de dados.
5. Meça de novo na mesma máquina e rode a suíte completa. Passe pelo `revisor`.

Formato de saída obrigatório:

1. **Problemas de desempenho** (medidos, com números)
2. **Estratégia de otimização**
3. **Código otimizado** (diff)
4. **Antes/depois** — tabela com as mesmas métricas
