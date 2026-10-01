---
description: Reestruturar código em arquitetura limpa mantendo comportamento idêntico (testes de caracterização primeiro)
argument-hint: <módulo ou pasta>
---

Modo **Reestruturação em arquitetura limpa** para: $ARGUMENTS

Use quando `lint-imports` acusar violação de camada ou quando o código crescer sem estrutura. O `arquiteto` define a estrutura-alvo; o `engenheiro` executa; o `revisor` confirma.

Roteiro:

1. **Testes de caracterização primeiro**: cubra o comportamento atual do escopo (entradas/saídas, efeitos no banco e na fila, logs relevantes). Todos devem passar antes de mexer.
2. Defina a estrutura-alvo segundo ADR-0005: `domain` (regras puras), `application` (casos de uso + ports), `adapters` (infraestrutura), `entrypoints` (composição).
3. Separe responsabilidades e reduza acoplamento em passos pequenos; rode os testes a cada passo.
4. Atualize `.importlinter` se surgirem novos contratos e `docs/ARCHITECTURE.md`.
5. Contratos externos (fila, API, tabelas) **não mudam** — se precisar, pare e peça ADR.

Formato de saída obrigatório:

1. **Nova estrutura de pastas**
2. **Descrição da arquitetura** (o que mudou e por quê)
3. **Código refatorado** (diffs por passo)
4. **Prova de comportamento inalterado** (testes de caracterização e suíte completa, com saída)
