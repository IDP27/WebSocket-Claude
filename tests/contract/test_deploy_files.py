"""Arquivos de deploy (systemd, Nginx, alertas) batem com o código (ADR-0020 §7)."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from ohip_streaming.config import ApiSettings, OhipSettings
from ohip_streaming.entrypoints.admin.settings import AdminSettings

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
NGINX = ROOT / "deploy" / "nginx" / "ohip-streaming.conf"
ALERTS = ROOT / "deploy" / "prometheus" / "ohip-alerts.yml"
SERVICES = sorted(SYSTEMD.glob("*.service"))


def unit(path: Path) -> dict[str, list[str]]:
    """Diretivas da unit (chave → valores, na ordem; comentários fora)."""
    values: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "[")) or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values.setdefault(key, []).append(value)
    return values


def one(path: Path, key: str) -> str:
    (value,) = unit(path)[key]
    return value


def test_every_service_runs_a_declared_script_hardened() -> None:
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert {p.name for p in SERVICES} == {
        "ohip-admin.service",
        "ohip-api.service",
        "ohip-consumer@.service",
        "ohip-enricher@.service",
        "ohip-publisher.service",
        "ohip-purge.service",
    }
    for path in SERVICES:
        values = unit(path)
        command = one(path, "ExecStart").split()[-1]
        assert command.startswith("/opt/ohip-streaming/.venv/bin/"), path.name
        assert Path(command).name in scripts, path.name
        assert values["User"] == ["ohip"], path.name
        for key, expected in {
            "NoNewPrivileges": "yes",
            "ProtectSystem": "strict",
            "ProtectHome": "yes",
            "PrivateTmp": "yes",
            "CapabilityBoundingSet": "",
        }.items():
            assert values[key] == [expected], (path.name, key)


def test_consumer_respects_the_ten_second_rule_and_drains() -> None:
    path = SYSTEMD / "ohip-consumer@.service"
    assert one(path, "Restart") == "always"
    assert one(path, "RestartSec") == "10"  # ADR-0007: também após crash/restart
    assert one(path, "StartLimitIntervalSec") == "0"
    # A instância da unit é a chain e vence o arquivo de ambiente.
    # %I: sem o escape do systemd (chain com espaço/símbolo); o arquivo de ambiente usa %i.
    assert one(path, "ExecStart").startswith("/usr/bin/env OHIP_CHAIN_CODE=%I ")
    assert "/etc/ohip/consumer-%i.env" in unit(path)["EnvironmentFile"]
    stop_s = int(one(path, "TimeoutStopSec"))
    assert stop_s > OhipSettings.model_fields["drain_timeout_s"].default + 30


@pytest.mark.parametrize("name", ["ohip-publisher.service", "ohip-enricher@.service"])
def test_misconfigured_broker_is_not_restarted(name: str) -> None:
    # main() devolve 2 com topologia divergente: reiniciar não resolve.
    assert one(SYSTEMD / name, "RestartPreventExitStatus") == "2"


def test_panel_never_gets_backend_credentials() -> None:
    # ADR-0004: o painel só fala com a API; nada de ORACLE_*/REDIS_*/RABBITMQ_*.
    assert unit(SYSTEMD / "ohip-admin.service")["EnvironmentFile"] == ["/etc/ohip/admin.env"]


def test_purge_is_a_daily_oneshot_with_its_own_credentials() -> None:
    service = SYSTEMD / "ohip-purge.service"
    assert one(service, "Type") == "oneshot"
    assert unit(service)["EnvironmentFile"] == ["/etc/ohip/purge.env"]  # usuário ohip_purge
    timer = unit(SYSTEMD / "ohip-purge.timer")
    assert timer["Persistent"] == ["true"]
    assert timer["Unit"] == ["ohip-purge.service"]


def test_no_secrets_in_deploy_files() -> None:
    secret = re.compile(
        r"(PASSWORD|SECRET|TOKEN|APP_KEY|CLIENT_SECRET)\w*\s*=\s*\S+|amqps?://\w+:\w+@|"
        r"redis://:\w+@",
        re.IGNORECASE,
    )
    for path in (ROOT / "deploy").rglob("*"):
        if path.is_file() and not path.name.startswith("."):  # .DS_Store do macOS
            text = path.read_text(encoding="utf-8")
            assert not secret.search(text), path


def _location(config: str, name: str) -> str:
    match = re.search(rf"location {re.escape(name)} \{{(.*?)\n    \}}", config, re.DOTALL)
    assert match, name
    return match.group(1)


def test_nginx_owns_the_panel_identity_headers() -> None:
    config = NGINX.read_text(encoding="utf-8")
    admin = AdminSettings.model_fields
    panel = _location(config, "/admin/")
    user_header = admin["user_header"].default
    groups_header = admin["groups_header"].default
    secret_header = admin["proxy_secret_header"].default
    assert f"proxy_set_header {user_header} $remote_user;" in panel
    assert f"proxy_set_header {groups_header} $ohip_groups;" in panel
    assert "include /etc/nginx/ohip/admin-proxy-secret.conf;" in panel
    # Sem grupo por padrão: com SSO corporativo, quem não está no mapa leva 403 (ADR-0018).
    groups_map = config.split("map $remote_user $ohip_groups {", 1)[1].split("}", 1)[0]
    assert 'default "";' in groups_map
    assert f"server 127.0.0.1:{admin['port'].default};" in config
    # Na API, os mesmos headers são apagados (identidade vem do token).
    api = _location(config, "/api/")
    for header in (user_header, groups_header, secret_header):
        assert f'proxy_set_header {header} "";' in api
    assert f"server 127.0.0.1:{ApiSettings.model_fields['port'].default};" in config


@pytest.mark.parametrize("endpoint", ["/health", "/ready", "/metrics"])
def test_internal_endpoints_are_restricted(endpoint: str) -> None:
    block = _location(NGINX.read_text(encoding="utf-8"), f"= {endpoint}")
    assert "deny all;" in block


def test_openapi_is_blocked_and_logs_skip_the_query_string() -> None:
    config = NGINX.read_text(encoding="utf-8")
    assert "location ~ ^/(docs|redoc|openapi\\.json) {\n        return 404;" in config
    assert "$request_uri" not in config.split("log_format", 1)[1].split(";", 1)[0]
    assert "access_log /var/log/nginx/ohip_access.log ohip_sem_query;" in config


def test_alert_rules_use_metrics_that_exist() -> None:
    source = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "src").rglob("*.py"))
    expressions = re.findall(r"^\s*expr: (.+)$", ALERTS.read_text(encoding="utf-8"), re.MULTILINE)
    assert expressions
    names = {name for expr in expressions for name in re.findall(r"\b(ohip_[a-z_]+)", expr)}
    missing = sorted(name for name in names if f'"{name}"' not in source)
    assert missing == []


# Contadores com rótulo imprevisível (não dá para registrar com 0) e limite alto o bastante
# para a primeira ocorrência não importar.
NOT_PREREGISTERED = {"ohip_merge_skipped_total"}  # rótulo event_name; limite > 100


def test_alert_counters_start_at_zero() -> None:
    """Todo contador das regras é registrado com 0 na partida do processo (ADR-0020 §6)."""
    from ohip_streaming.entrypoints import consumer, enricher, publisher

    registered = {
        name
        for name, _ in [
            *consumer.alert_counters("CHAIN1"),
            *publisher.alert_counters(),
            *enricher.alert_counters(),
        ]
    }
    expressions = re.findall(r"^\s*expr: (.+)$", ALERTS.read_text(encoding="utf-8"), re.MULTILINE)
    counters = {n for e in expressions for n in re.findall(r"\b(ohip_[a-z_]+_total)\b", e)}
    assert sorted(counters - registered - NOT_PREREGISTERED) == []


def test_rare_event_rules_see_the_first_occurrence() -> None:
    rare = (
        "ohip_lease_lost_total",
        "ohip_connection_poisoned_total",
        "ohip_card_data_detected_total",
        "ohip_enricher_systemic_failures_total",
        "ohip_enricher_unexpected_errors_total",
    )
    text = ALERTS.read_text(encoding="utf-8")
    for name in rare:
        assert re.search(rf"{name} > 0 unless {name} offset \d+[mh]", text), name
