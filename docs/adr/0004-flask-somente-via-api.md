# ADR-0004 — Painel Flask consome somente a API FastAPI

- Status: Aceito (aprovado em 2026-10-01 com a Fase 0)
- Data: 2026-10-01

## Contexto

O PRD exige FastAPI (API de controle) e Flask (painel). Se os dois acessassem o Oracle, as regras de autorização, paginação e mascaramento LGPD ficariam duplicadas.

## Decisão

- O `ohip-admin` não importa `oracledb` nem `redis`; usa um cliente HTTP para a `ohip-api`.
- O painel tem **dois** tokens de serviço, `read` e `admin`, e escolhe o token pelo grupo do usuário (vindo do Nginx/SSO). A API é quem decide o que cada token pode fazer. O Flask também confere o grupo no servidor em todo POST (defesa em profundidade) e repassa o usuário em `X-Actor` para auditoria.
- `import-linter` impede `entrypoints.admin` de importar `adapters.oracle`, `adapters.redis`, `oracledb` e `redis`.

## Consequências

- Uma única fonte de regras e de autorização.
- O painel fica indisponível se a API cair; aceitável para uma ferramenta interna (a API é leve e tem vários workers).
- Os tokens de serviço do painel são segredos a proteger (cofre/ambiente).
