-- =============================================================================
-- 003_seed_chain.sql — Lease, offset e status de uma chain (ADR-0008)
-- Alvo: PostgreSQL 15+ (ADR-0021). Tradução do bloco "por chain" de sql/003_seed_leases.sql.
-- RASCUNHO DA FASE 11 — aplicado pelo DBA a cada chain nova, como ohip_owner:
--   psql -v ON_ERROR_STOP=1 --single-transaction -v chain_code=XXXX -f 003_seed_chain.sql
-- Sem -v chain_code o psql para no primeiro comando e nada é gravado.
-- =============================================================================

INSERT INTO ohip_lease (lease_name, epoch, expires_at)
VALUES ('consumer:' || :'chain_code', 0, now());

INSERT INTO ohip_offset (chain_code, last_offset) VALUES (:'chain_code', NULL);

INSERT INTO ohip_consumer_status (chain_code, state) VALUES (:'chain_code', 'STOPPED');
