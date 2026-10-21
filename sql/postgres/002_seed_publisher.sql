-- =============================================================================
-- 002_seed_publisher.sql — Lease do publisher (ADR-0008)
-- Alvo: PostgreSQL 15+ (ADR-0021). Tradução da primeira linha de sql/003_seed_leases.sql.
-- RASCUNHO DA FASE 11 — aplicado pelo DBA uma vez, como ohip_owner (search_path=ohip):
--   psql -v ON_ERROR_STOP=1 --single-transaction -f 002_seed_publisher.sql
-- As linhas de OHIP_LEASE são criadas no provisionamento, nunca em tempo de execução.
-- =============================================================================

INSERT INTO ohip_lease (lease_name, epoch, expires_at)
VALUES ('publisher', 0, now())
ON CONFLICT (lease_name) DO NOTHING;
