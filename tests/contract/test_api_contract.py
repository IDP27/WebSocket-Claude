"""Contrato da API: o OpenAPI gerado bate com a tabela de endpoints do docs/API.md (RNF-15).

Rota nova, removida ou com perfil diferente falha aqui: mudar o contrato exige ADR.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from tests.unit.entrypoints.test_api_routes import ADMIN, READ, make_env

from ohip_streaming.entrypoints.api.app import build_app

API_MD = Path(__file__).resolve().parents[2] / "docs" / "API.md"
_ROW_RE = re.compile(r"^\| (GET|POST|DELETE|PUT|PATCH) \| `([^`]+)` \| (\w+) \|", re.MULTILINE)
_PARAM_RE = re.compile(r"\{[^}]+\}")


def documented() -> list[tuple[str, str, str]]:
    """(método, rota com parâmetros genéricos, perfil) de cada linha da tabela."""
    return [
        (method, _PARAM_RE.sub("{}", path), profile)
        for method, path, profile in _ROW_RE.findall(API_MD.read_text())
    ]


def implemented() -> set[tuple[str, str]]:
    paths = build_app().openapi()["paths"]
    return {
        (method.upper(), _PARAM_RE.sub("{}", path))
        for path, operations in paths.items()
        for method in operations
    }


def test_routes_match_the_documented_table() -> None:
    table = documented()
    assert len(table) == 14
    assert {(m, p) for m, p, _ in table} == implemented()


def _url(path: str) -> str:
    return path.replace("{}", "1")


@pytest.mark.parametrize(("method", "path", "profile"), documented())
def test_profiles_are_enforced(method: str, path: str, profile: str) -> None:
    client = make_env().client
    anonymous = client.request(method, _url(path))
    as_reader = client.request(method, _url(path), headers={**READ, "X-Actor": "ana"})
    if profile == "interno":
        assert anonymous.status_code != 401
    elif profile == "read":
        assert anonymous.status_code == 401
        assert as_reader.status_code not in (401, 403)
    else:
        assert profile == "admin"
        assert anonymous.status_code == 401
        assert as_reader.status_code == 403
        as_admin = client.request(method, _url(path), headers=ADMIN)
        assert as_admin.status_code not in (401, 403)


def test_openapi_documents_the_error_format() -> None:
    schema = build_app().openapi()
    assert "ErrorBody" in schema["components"]["schemas"]
    assert schema["paths"]["/api/v1/replay"]["post"]["responses"]["409"]
