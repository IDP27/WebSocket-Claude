# ADR-0018 — Painel operacional Flask (Fase 8)

- Status: Proposto (Fase 8)
- Data: 2026-10-05

## Contexto

O painel foi desenhado na Fase 0 (docs/UI.md) e só fala com a API (ADR-0004). A API ficou pronta na Fase 7 (ADR-0017). A Q-5 (forma de login) segue aberta, com o padrão: autenticação no Nginx/SSO e usuário/grupo em headers definidos pelo Nginx. O import-linter proíbe o painel de importar `ohip_streaming.config`, os adapters de Oracle e Redis, `oracledb` e `redis`.

## Decisão

1. **Pacote `entrypoints/admin/`**, sem dependência nova (Flask, Jinja2, Gunicorn e `httpx` já estão na stack):
   - `settings.py`: configuração própria do painel (`ADMIN_*`), mais `APP_ENVIRONMENT`/`APP_CODE_VERSION` e `LOG_*` lidos por classes do próprio painel (o painel nunca carrega `OracleSettings`);
   - `api_client.py`: cliente síncrono da API sobre `httpx.Client` (Gunicorn com workers síncronos);
   - `app.py`: fábrica `create_app()` com o blueprint em `/admin`; `server.py`: processo `ohip-admin` (Gunicorn embutido).
2. **Identidade do usuário (Q-5, padrão)**:
   - o Nginx autentica e grava o usuário em `ADMIN_USER_HEADER` (padrão `X-Forwarded-User`) e os grupos em `ADMIN_GROUPS_HEADER` (padrão `X-Forwarded-Groups`, separados por vírgula), **sobrescrevendo** o que vier do cliente (inclusive com valor vazio);
   - os headers só são confiáveis se só o Nginx alcança o painel: `ADMIN_HOST` precisa ser endereço local (`127.0.0.1`, `::1`, `localhost`), senão a configuração exige `ADMIN_PROXY_SECRET` (32+ caracteres), que o Nginx envia em `ADMIN_PROXY_SECRET_HEADER` e o painel confere com `hmac.compare_digest` (sem ele → 403). O segredo também fecha o acesso direto a partir de outros processos da VM;
   - o usuário vai à API em `X-Actor` (header HTTP, só ASCII) e é gravado em `VARCHAR2(100)`: precisa ser ASCII imprimível com até 100 caracteres; outro formato → 400 com mensagem para ajustar o header no Nginx/SSO (Q-5);
   - grupo `ADMIN_ADMIN_GROUP` → perfil `admin`; grupo `ADMIN_READ_GROUP` → `read`; nenhum dos dois → 403; sem usuário → 401 (Nginx mal configurado);
   - o painel escolhe o token de serviço pelo perfil (`ADMIN_API_READ_TOKEN` ou `ADMIN_API_ADMIN_TOKEN`) e repassa o usuário em `X-Actor`. Todo POST confere o perfil no servidor antes de chamar a API; a autorização final é da API.
3. **CSRF** sem dependência nova: token aleatório na sessão assinada do Flask (`ADMIN_SECRET_KEY`), campo oculto em todo formulário e comparação com `hmac.compare_digest`. Cookie `HttpOnly`, `SameSite=Strict` e `Secure` (fora de desenvolvimento).
4. **HTMX só para atualizar fragmentos**:
   - os blocos de status, DLQ e histórico de replay usam `hx-get` + `hx-trigger="every 15s"` em rotas `/admin/fragments/...`; o link de nova tentativa de um fragmento com erro leva só os filtros conhecidos daquele bloco;
   - o painel funciona sem JavaScript: formulários são POST comuns e a navegação é por links;
   - o `htmx.min.js` 2.0.11 é servido pelo próprio painel (`/admin/static/`), sem CDN, com `integrity` (SHA-384 publicado em htmx.org; origem em `static/HTMX_PROVENANCE.md`, conferida por teste);
   - login do SSO vencido durante a atualização automática: o Nginx deve responder 401 (não redirecionar) a requisições com `HX-Request: true`, porque o HTMX seguiria o redirecionamento e trocaria o bloco pela página de login. Se a requisição chegar ao painel sem usuário, o painel responde 401 com `HX-Refresh: true` (o navegador recarrega a página inteira e passa pelo login);
   - `Content-Security-Policy: default-src 'self'`, sem script nem estilo inline (o HTMX recebe `includeIndicatorStyles: false`).
5. **Confirmação dupla do replay sem modal**: o UI.md previa um modal; sem JavaScript próprio, a confirmação é uma **segunda página**. O operador preenche chain, offset e motivo; a página de confirmação mostra o resumo e o aviso e exige digitar o código da chain. O painel confere o texto e a API confere de novo (`confirm`).
6. **Erros da API**: em páginas, um aviso com `code`, mensagem e `request_id`; em ações, mensagem flash e volta para a página de origem. API fora ou lenta (`ADMIN_API_TIMEOUT_S`) → página 503 com o `request_id` do painel. O painel envia `X-Request-ID` próprio por requisição e o repassa à API, para correlacionar os logs.
7. **LGPD**: o painel só mostra o que a API devolve (sempre mascarado); não oferece `unmasked`. O export baixa o JSON mascarado da API.
8. **Logs**: uma linha por requisição (método, rota sem query string, status, duração, usuário, perfil, `request_id`). O access log do Gunicorn fica desligado (grava a query string). `httpx`/`httpcore` ficam em `WARNING` em todos os processos: o log `INFO` deles grava a URL completa.
9. **Cabeçalhos de segurança**: CSP, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer` e `Cache-Control: no-store` nas páginas.

## Consequências

- Variáveis novas `ADMIN_*` no `.env.example`; o teste do `.env.example` passa a ler também as classes de configuração do painel.
- A Q-5 continua aberta: trocar o login (LDAP, SSO com outros headers) muda só a configuração dos headers e o Nginx (Fase 10).
- O painel depende da API: API fora → páginas 503 (aceito no ADR-0004).
