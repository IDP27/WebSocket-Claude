---
description: Mapear a arquitetura real do código existente e apontar problemas estruturais, duplicação, gargalos e riscos
argument-hint: <módulo, pasta ou "tudo">
---

Modo **Compreensão e refatoração** sobre: $ARGUMENTS

Use o subagente `revisor` para o diagnóstico (somente leitura). Mudanças de código só depois da minha aprovação, pelo `engenheiro`, com testes de caracterização antes (RNF-15).

Roteiro:

1. Leia o código do escopo inteiro e os testes. Rode `lint-imports`, `mypy` e `pytest` para ter o estado atual.
2. Mapeie a arquitetura **real** (não a documentada): módulos, dependências, fluxo de dados. Compare com `docs/ARCHITECTURE.md` e os ADRs.
3. Encontre: violações de camada, duplicação, acoplamento, gargalos, código morto, testes faltando, riscos de manutenção, desvios do PRD.
4. Proponha uma estratégia de refatoração em passos pequenos, cada um com teste que prova comportamento inalterado.

Formato de saída obrigatório:

1. **Resumo da arquitetura** (real × documentada)
2. **Áreas problemáticas** — tabela com severidade (crítico/alto/médio/baixo), arquivo:linha, problema e evidência
3. **Estratégia de refatoração** — passos ordenados, com risco e teste de caracterização de cada um
4. **Código aprimorado** — somente após aprovação; diffs pequenos por passo
