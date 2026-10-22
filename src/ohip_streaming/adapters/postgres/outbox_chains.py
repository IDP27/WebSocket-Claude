"""Chains presentes no outbox, sem varrer a tabela (diagnóstico pós-Fase 11, bug B2).

As leituras de "pendente" (publisher a cada rodada, ``/status``, ``/metrics``) filtravam só
``status``. O índice ``ohip_outbox_ix_head (chain_code, status, id)`` começa por
``chain_code`` e o PostgreSQL 15/16 não tem *skip scan*: o plano virava varredura completa
do outbox, cujo tamanho é o histórico retido (90 dias de ``SENT``), não o que está pendente.

``OUTBOX_CHAINS_CTE`` faz a varredura "pulando" pelo índice: cada passo é um ``min()`` com
``chain_code >`` o anterior (uma descida no índice). Depois, cada leitura consulta
``(chain_code, status)`` exato, o que o índice atende direto. O custo passa a depender do
número de chains e das linhas pendentes. Não depende do provisionamento em ``ohip_offset``:
uma chain sem linha lá ainda aparece se tiver mensagens.
"""

from __future__ import annotations

from typing import Final

OUTBOX_CHAINS_CTE: Final = """
WITH RECURSIVE chains AS (
    SELECT min(chain_code) AS chain_code FROM ohip_outbox
    UNION ALL
    SELECT (SELECT min(o.chain_code) FROM ohip_outbox o WHERE o.chain_code > c.chain_code)
      FROM chains c
     WHERE c.chain_code IS NOT NULL
)"""
