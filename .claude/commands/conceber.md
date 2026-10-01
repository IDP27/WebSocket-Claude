---
description: Projetar uma nova funcionalidade ou componente antes de codar (arquitetura → MVP escalável)
argument-hint: <funcionalidade ou componente>
---

Modo **Concepção e MVP escalável** para: $ARGUMENTS

Use o subagente `arquiteto` para o desenho e, só depois da minha aprovação do desenho, o `engenheiro` para o código. Leia antes `CLAUDE.md`, `docs/ARCHITECTURE.md` e os ADRs.

Roteiro:

1. Entenda o objetivo e o encaixe no PRD e no plano de fases. Liste premissas e perguntas.
2. Projete antes de codar: componentes, responsabilidades, ports, fluxo de dados, contratos, esquema do banco (Oracle 11g), cache, UI (se houver).
3. Registre decisões relevantes em ADR. Divergências com o PRD ou com a doc Oracle viram itens em `docs/OPEN_QUESTIONS.md`.
4. Defina o MVP: o menor recorte pronto para produção que não precise ser reconstruído depois.
5. Pare para aprovação. Depois, implemente com testes e passe pelo `revisor`.

Formato de saída obrigatório (seções nesta ordem):

1. **Arquitetura**
2. **Estrutura de arquivos**
3. **Esquema do banco**
4. **Endpoints**
5. **Arquitetura da UI**
6. **Fluxo de dados**
7. **Estratégia de cache**
8. **Código** (somente após aprovação do desenho)

Seções que não se aplicam: escreva "não se aplica" e o motivo em uma linha.
