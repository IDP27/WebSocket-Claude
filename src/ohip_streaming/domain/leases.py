"""Nomes dos leases de liderança (ADR-0008), numa fonte só (refatoração 10).

O ``LeaseKeeper`` dos entrypoints disputa o lease por este nome, e a barreira de epoch dos
adapters (``UPDATE ohip_lease ... WHERE lease_name = ... AND epoch = ...``) confere o mesmo
nome. Se os dois lados divergissem, toda escrita viraria ``LeaseLostError``. As linhas de
``OHIP_LEASE`` são semeadas no provisionamento com estes nomes (``sql/postgres/``).
"""

from __future__ import annotations

from typing import Final

PUBLISHER_LEASE: Final = "publisher"


def consumer_lease(chain_code: str) -> str:
    """Lease do consumer de uma chain (um consumer por appKey, chain e gateway)."""
    return f"consumer:{chain_code}"
