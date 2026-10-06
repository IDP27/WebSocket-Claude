"""Backoff exponencial com teto, único para todos os processos (refatoração 1 do /entender).

Antes, cada componente tinha a sua fórmula ``inicial * 2 ** (n - 1)``; com milhares de falhas
seguidas (broker ou banco fora por ~17 h, uma falha por minuto) ``2 ** 1100`` não cabe num
float e a conta estourava com ``OverflowError``, derrubando o processo. Aqui o expoente tem teto.
"""

from __future__ import annotations

from typing import Final

# 2**62 já passa de qualquer teto em segundos; o float só estoura perto de 2**1024.
MAX_EXPONENT: Final = 62


def exponential_backoff(attempt: int, initial_s: float, max_s: float) -> float:
    """``initial_s * 2 ** (attempt - 1)``, no máximo ``max_s``. ``attempt`` < 1 conta como 1."""
    exponent = min(max(0, attempt - 1), MAX_EXPONENT)
    return min(max_s, initial_s * float(2**exponent))
