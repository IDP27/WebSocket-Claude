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
5. **Logs**: structlog com saída JSON em stdout (journald em produção). Campos fixos `service`, `environment`, `code_version`; correlação por `bind_context` (contextvars, seguro com asyncio); logs de bibliotecas passam pelo mesmo formatador; segredos mascarados por nome de campo, por `SecretStr` e por padrão (`Bearer ...`, senha em URL).
6. **Envio ao MongoDB de logs**: fora do processo. O serviço só escreve em stdout; o coletor que o time já usa leva do journald ao MongoDB (Q-16). Evita dependência nova (`pymongo`) e evita que uma falha no MongoDB afete a ingestão.
7. **Regras do ruff** incluem `S` (bandit), `DTZ` (datetime sem fuso) e `ASYNC`, que pegam erros comuns neste tipo de serviço. mypy em modo `strict`.
8. **Contratos do import-linter** além do ADR-0005: `domain` não importa `structlog` nem `pydantic` (domínio puro, com dataclasses); `domain`/`application` não importam `ohip_streaming.config` (recebem valores, não configuração); `entrypoints.api` e `entrypoints.admin` são independentes.

## Consequências

- `PYTHONPATH=src` é definido no `Makefile` e no teste de arquitetura: no macOS, se o `.pth` da instalação editável ganhar a flag `hidden`, o Python 3.12+ o ignora (visto nesta fase; ver README).
- Mudança na forma das variáveis é mudança de contrato operacional: atualizar `.env.example` (o teste falha se esquecer).
