"""Apoio comum aos processos (sem dependências de um processo específico)."""

from __future__ import annotations

import os
import socket
import uuid


def instance_id() -> str:
    """Identificação da instância no lease e nos logs: ``host:pid:aleatório``."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
