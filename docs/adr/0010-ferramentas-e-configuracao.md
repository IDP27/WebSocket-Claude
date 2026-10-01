# ADR-0010 — Ferramentas de desenvolvimento, configuração e logs (Fase 1)

- Status: Proposto (Fase 1)
- Data: 2026-10-01

## Contexto

A Fase 1 monta o esqueleto: empacotamento, configuração por ambiente, logs e portões de qualidade. Algumas escolhas não estavam explícitas no PRD.

## Decisão

1. **pytest-cov** no grupo `dev` para medir a cobertura exigida (≥ 80%). É o plugin padrão do pytest para isso; não entra em produção.
2. **Configuração em grupos** (`AppSettings`, `OhipSettings`, `OracleSettings`, ...), cada um com prefixo próprio. Cada entrypoint carrega só o que usa (`load_settings(OracleSettings)`); o painel nunca recebe credenciais do Oracle. Os limites do protocolo OHIP (ADR-0007) são validados na carga: uma configuração que provocaria 4408/4409 não sobe.
3. **Listas em variáveis de ambiente** aceitam texto separado por vírgula (`OHIP_HOTEL_CODES=H1,H2`); mapas usam JSON (`OHIP_MODULE_CODES`, `API_SERVICE_TOKENS`).
4. **`.env.example` sem comentário na mesma linha do valor**, porque o `EnvironmentFile` do systemd incluiria o comentário no valor. Um teste garante isso e garante que toda variável de configuração está documentada nele.
5. **Logs**: structlog com saída JSON em stdout (journald em produção). Campos fixos `service`, `environment`, `code_version`; correlação por `bind_context` (contextvars, seguro com asyncio); logs de bibliotecas passam pelo mesmo formatador.
   - Segredos mascarados por nome de campo (snake, kebab e camelCase, por nome exato ou terminação: `*secret`, `*password`, `*token`, `*appkey`, `*apikey`), por `SecretStr` e por padrão no texto (`Bearer ...`, senha em URL, `?key=<sha256>`, `x-app-key: ...`).
   - **Tracebacks sem variáveis locais** (`show_locals=False`; no console, `format_exc_info` em texto simples), sempre convertidos antes da máscara. O padrão do structlog serializa os locais, e o `repr` deles escapava da máscara (achado crítico da revisão da Fase 1).
   - Saída legível (`LOG_JSON_OUTPUT=false`) só é aceita em `desenvolvimento`.
   - Código síncrono em thread (Oracle) roda via `ohip_streaming.logging.run_in_executor`, que copia os contextvars; `loop.run_in_executor` puro perderia o `unique_event_id`.
6. **Envio ao MongoDB de logs**: fora do processo. O serviço só escreve em stdout; o coletor que o time já usa leva do journald ao MongoDB (Q-16). Evita dependência nova (`pymongo`) e evita que uma falha no MongoDB afete a ingestão.
7. **Regras do ruff** incluem `S` (bandit), `DTZ` (datetime sem fuso) e `ASYNC`, que pegam erros comuns neste tipo de serviço. mypy em modo `strict`.
8. **Contratos do import-linter** além do ADR-0005: `domain` não importa `structlog` nem `pydantic` (domínio puro, com dataclasses); `domain`/`application` não importam `ohip_streaming.config` (recebem valores, não configuração); `entrypoints.api` e `entrypoints.admin` são independentes.
9. Logs não carregam dados pessoais nem payloads inteiros: o `detail` só entra em log já mascarado pelo domínio (Fase 2).

## Pendências registradas para fases futuras

- **Fase 7/8**: uvicorn e gunicorn reconfiguram os próprios loggers na partida; usar `uvicorn.run(..., log_config=None)` e `logconfig_dict` no gunicorn para manter um único formato.
- **Fase 8**: o painel terá configuração própria (URL da API, tokens); o import-linter já proíbe `entrypoints.admin` de importar `ohip_streaming.config`.
- **Fases 6, 9 e 10**: grupos de configuração do publisher (lease próprio, backoff global), do enricher, do expurgo e do snapshot de métricas; avaliar um `LeaseSettings` comum a consumer e publisher.
- **Fase 10**: `chain_code` pode ter espaço ou `%#$&` (padrão do OHIP); o nome da unit `ohip-consumer@<chain>` precisa de `systemd-escape`.

## Consequências

- `PYTHONPATH=src` é definido no `Makefile` e no teste de arquitetura: no macOS, se o `.pth` da instalação editável ganhar a flag `hidden`, o Python 3.12+ o ignora (visto nesta fase; ver README).
- Mudança na forma das variáveis é mudança de contrato operacional: atualizar `.env.example` (o teste falha se esquecer).
