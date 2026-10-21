-- =============================================================================
-- 900_grants_example.sql — Papéis com privilégio mínimo (RNF-07)
-- Alvo: PostgreSQL 15+ (ADR-0021). Tradução de sql/900_grants_example.sql.
-- RASCUNHO DA FASE 11 — NÃO EXECUTAR. Ajustar nomes e schema com o DBA.
--
-- ohip_owner : dono do schema ohip (só para DDL; nenhum processo usa este papel).
-- ohip_app   : consumer, publisher, enricher e API.
-- ohip_purge : expurgo (SELECT para os filtros NOT EXISTS, DELETE nas três tabelas).
-- ohip_read  : leitura para BI (opcional).
-- =============================================================================

-- CREATE ROLE ohip_owner NOLOGIN;
-- CREATE SCHEMA ohip AUTHORIZATION ohip_owner;
-- Os processos acham as tabelas pelo search_path do papel (sem prefixo no SQL):
-- ALTER ROLE ohip_app   SET search_path = ohip;
-- ALTER ROLE ohip_purge SET search_path = ohip;
-- REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- CREATE ROLE ohip_app LOGIN PASSWORD '***';   -- senha no cofre, nunca no repositório
-- GRANT USAGE ON SCHEMA ohip TO ohip_app;
-- GRANT SELECT, INSERT, UPDATE ON ohip.ohip_event_raw, ohip.ohip_offset, ohip.ohip_lease,
--     ohip.ohip_outbox, ohip.ohip_dlq, ohip.ohip_consumer_status, ohip.ohip_replay_request
--     TO ohip_app;
-- GRANT USAGE ON SEQUENCE ohip.ohip_event_raw_seq, ohip.ohip_outbox_seq, ohip.ohip_dlq_seq,
--     ohip.ohip_replay_req_seq TO ohip_app;

-- CREATE ROLE ohip_purge LOGIN PASSWORD '***';
-- GRANT USAGE ON SCHEMA ohip TO ohip_purge;
-- GRANT SELECT, DELETE ON ohip.ohip_event_raw, ohip.ohip_outbox, ohip.ohip_dlq TO ohip_purge;

-- BI: NUNCA ler OHIP_EVENT_RAW direto (payload com dados pessoais).
-- Dar acesso a uma view mascarada (definida após a Q-9) e às tabelas de domínio (Q-1).
-- CREATE ROLE ohip_read LOGIN PASSWORD '***';
-- GRANT USAGE ON SCHEMA ohip TO ohip_read;
-- GRANT SELECT ON ohip.ohip_event_raw_v TO ohip_read;   -- view mascarada (a criar)
