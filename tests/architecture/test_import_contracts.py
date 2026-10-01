"""Regra de dependência entre camadas (RNF-14): o import-linter roda junto com os testes."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_import_contracts_are_kept() -> None:
    executable = shutil.which("lint-imports", path=str(Path(sys.executable).parent))
    assert executable, "lint-imports não encontrado no ambiente virtual"

    # PYTHONPATH explícito: não depende do .pth da instalação editável (no macOS, um .pth com a
    # flag "hidden" é ignorado pelo Python 3.12+).
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    result = subprocess.run(  # noqa: S603 - executável do próprio venv, sem shell
        [executable, "--config", str(ROOT / ".importlinter")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
