"""A coleção Postman do projeto bate com as specs oficiais e o environment não carrega segredo."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
VENDOR = ROOT / "vendor" / "oracle-hospitality-api-docs" / "rest-api-specs"
COLLECTION = ROOT / "postman" / "ohip-streaming.postman_collection.json"
ENVIRONMENT = ROOT / "postman" / "ohip-streaming.postman_environment.json"
SPECS = {
    "oauth": VENDOR / "security" / "v1" / "publishedoauth.json",
    "rsv": VENDOR / "property" / "v1" / "rsv.json",
    "crm": VENDOR / "property" / "v1" / "crm.json",
}
SECRET_KEYS = {"AppKey", "CLIENT_SECRET", "Password", "Token"}


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _requests(items: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    for item in items:
        if "item" in item:
            yield from _requests(item["item"])
        else:
            yield item


REQUESTS = list(_requests(_load(COLLECTION)["item"]))


def _spec_operation(method: str, path: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Acha a operação da spec que casa com o caminho da coleção (variáveis = parâmetros)."""
    spec = _load(SPECS[path[0]])
    base = spec["basePath"].strip("/").split("/")
    rest = path[len(base) :]
    assert path[: len(base)] == base, f"{path} fora do basePath {spec['basePath']}"
    for template, operations in spec["paths"].items():
        parts = template.strip("/").split("/")
        matches = len(parts) == len(rest) and all(
            p == r or (p.startswith("{") and r.startswith("{{"))
            for p, r in zip(parts, rest, strict=True)
        )
        if matches and method.lower() in operations:
            return spec, operations[method.lower()]
    raise AssertionError(f"{method} /{'/'.join(path)} não existe na spec")


def _required_headers(spec: dict[str, Any], operation: dict[str, Any]) -> set[str]:
    names = set()
    for param in operation.get("parameters", []):
        if "$ref" in param:
            param = spec["parameters"][param["$ref"].split("/")[-1]]
        if param["in"] == "header" and param.get("required"):
            names.add(param["name"].lower())
    return names


@pytest.mark.parametrize("item", REQUESTS, ids=[r["name"] for r in REQUESTS])
def test_request_exists_in_the_official_spec_with_required_headers(item: dict[str, Any]) -> None:
    request = item["request"]
    path = [p for p in request["url"]["path"]]
    spec, operation = _spec_operation(request["method"], path)
    sent = {h["key"].lower() for h in request["header"]}
    if request.get("auth", {}).get("type") == "basic":
        sent.add("authorization")
    assert _required_headers(spec, operation) <= sent


def test_environment_template_has_no_values() -> None:
    values = _load(ENVIRONMENT)["values"]
    assert all(v["value"] == "" for v in values), "environment do repositório deve vir vazio"
    secrets = {v["key"] for v in values if v["type"] == "secret"}
    assert secrets >= SECRET_KEYS


def test_collection_has_no_literal_credentials() -> None:
    text = COLLECTION.read_text(encoding="utf-8")
    assert not re.search(r"Bearer (?!\{\{)", text)
    assert not re.search(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        text.replace("6f1c0a52-3c1e-4c8e-9d2b-0b7a5f0e1a11", ""),
    )  # nenhuma app key (UUID) além do id da coleção
    every_variable = set(re.findall(r"\{\{(\$?\w+)\}\}", text)) - {"$guid"}
    declared = {v["key"] for v in _load(ENVIRONMENT)["values"]}
    assert every_variable <= declared, f"variáveis sem declaração: {every_variable - declared}"
