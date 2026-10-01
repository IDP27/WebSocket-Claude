-- =============================================================================
-- 001_sequences.sql — Sequences do consumidor OHIP Streaming
-- Alvo: Oracle 11g (sem IDENTITY e sem DEFAULT seq.NEXTVAL).
-- RASCUNHO DA FASE 0 — NÃO EXECUTAR sem aprovação.
--
-- As sequences são usadas explicitamente no INSERT pela aplicação.
-- OHIP_OUTBOX_SEQ define a ordem de publicação por chain. Se o banco for RAC,
-- trocar NOORDER por ORDER nessa sequence (ver docs/OPEN_QUESTIONS.md, Q-11).
-- =============================================================================

CREATE SEQUENCE ohip_event_raw_seq START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE NOORDER;

CREATE SEQUENCE ohip_outbox_seq START WITH 1 INCREMENT BY 1 CACHE 1000 NOCYCLE NOORDER;

CREATE SEQUENCE ohip_dlq_seq START WITH 1 INCREMENT BY 1 CACHE 20 NOCYCLE NOORDER;

CREATE SEQUENCE ohip_replay_req_seq START WITH 1 INCREMENT BY 1 CACHE 20 NOCYCLE NOORDER;
