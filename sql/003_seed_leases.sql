-- =============================================================================
-- 003_seed_leases.sql — Linhas de lease e offset (ADR-0008)
-- RASCUNHO DA FASE 0 — NÃO EXECUTAR sem aprovação.
--
-- As linhas de OHIP_LEASE são criadas no provisionamento, nunca por MERGE em
-- tempo de execução (dois MERGE concorrentes numa linha inexistente geram ORA-00001).
-- Ao adicionar uma chain: rodar o bloco "por chain" com o chain_code novo.
-- =============================================================================

INSERT INTO ohip_lease (lease_name, epoch, expires_at)
VALUES ('publisher', 0, SYS_EXTRACT_UTC(SYSTIMESTAMP));

-- Por chain (substituir &chain_code):
INSERT INTO ohip_lease (lease_name, epoch, expires_at)
VALUES ('consumer:' || '&chain_code', 0, SYS_EXTRACT_UTC(SYSTIMESTAMP));

INSERT INTO ohip_offset (chain_code, last_offset) VALUES ('&chain_code', NULL);

INSERT INTO ohip_consumer_status (chain_code, state) VALUES ('&chain_code', 'STOPPED');

COMMIT;
