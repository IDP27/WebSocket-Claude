-- =============================================================================
-- 002_core_tables.sql — Tabelas centrais do consumidor OHIP Streaming
-- Alvo: Oracle 11g. Identificadores <= 30 caracteres. Payload JSON em CLOB.
-- Datas sempre em UTC (TIMESTAMP sem fuso; a aplicação grava UTC explícito).
-- RASCUNHO DA FASE 0 — NÃO EXECUTAR sem aprovação.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Evento bruto: fonte da verdade. Uma linha por uniqueEventId.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_event_raw (
    id                  NUMBER(19)      NOT NULL,
    chain_code          VARCHAR2(20)    NOT NULL,
    hotel_id            VARCHAR2(50),                       -- pode vir nulo (ex.: perfis compartilhados)
    offset_value        VARCHAR2(20)    NOT NULL,           -- string ^[0-9]+$ (schema OHIP); nunca tratar como número no protocolo
    unique_event_id     VARCHAR2(64)    NOT NULL,
    subscription_id     VARCHAR2(36),                       -- GUID do subscribe que entregou o evento
    module_name         VARCHAR2(100)   NOT NULL,
    event_name          VARCHAR2(100)   NOT NULL,
    primary_key         VARCHAR2(100)   NOT NULL,
    publisher_id        VARCHAR2(50),
    action_instance_id  VARCHAR2(50),
    event_ts            TIMESTAMP(3),                       -- campo "timestamp" do OHIP; fuso a confirmar (D-3)
    payload             CLOB            NOT NULL,           -- JSON do newEvent, como recebido
    processing_status   VARCHAR2(20)    DEFAULT 'RECEIVED' NOT NULL,
    received_at         TIMESTAMP(6)    NOT NULL,           -- chegada no consumer (UTC)
    persisted_at        TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    updated_at          TIMESTAMP(6),
    CONSTRAINT ohip_event_raw_pk PRIMARY KEY (id),
    CONSTRAINT ohip_event_raw_uq_evt UNIQUE (unique_event_id),
    CONSTRAINT ohip_event_raw_ck_status CHECK (
        processing_status IN ('RECEIVED', 'IGNORED', 'NORMALIZED', 'UNMAPPED', 'ENRICHED', 'FAILED')
    ),
    -- Guia Oracle (Troubleshooting): duplicado = mesmo primaryKey e offset (ADR-0009)
    CONSTRAINT ohip_event_raw_uq_off UNIQUE (chain_code, offset_value, primary_key)
);

CREATE INDEX ohip_event_raw_ix_evt_recv  ON ohip_event_raw (event_name, received_at);
CREATE INDEX ohip_event_raw_ix_hotel_pk  ON ohip_event_raw (hotel_id, primary_key);
CREATE INDEX ohip_event_raw_ix_recv      ON ohip_event_raw (received_at);  -- expurgo por data

-- -----------------------------------------------------------------------------
-- Último offset confirmado por chain. Atualizado só dentro da transação do lote
-- (ou do replay aplicado), sempre após conferir o epoch em OHIP_LEASE.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_offset (
    chain_code              VARCHAR2(20)    NOT NULL,
    last_offset             VARCHAR2(20),
    last_unique_event_id    VARCHAR2(64),
    updated_at              TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    CONSTRAINT ohip_offset_pk PRIMARY KEY (chain_code)
);

-- -----------------------------------------------------------------------------
-- Liderança com prazo e epoch (ADR-0008). lease_name: 'consumer:<chain>' ou 'publisher'.
-- Linhas semeadas no provisionamento (003_seed_leases.sql); não há MERGE concorrente.
-- Aquisição (hora do banco, evita relógio de VMs diferentes):
--   UPDATE ohip_lease SET epoch = epoch + 1, owner = :me,
--          acquired_at = SYS_EXTRACT_UTC(SYSTIMESTAMP),
--          expires_at  = SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTODSINTERVAL(:ttl, 'SECOND')
--    WHERE lease_name = :n AND (expires_at < SYS_EXTRACT_UTC(SYSTIMESTAMP) OR owner = :me)
-- Renovação (conexão de controle): UPDATE de expires_at WHERE owner = :me AND epoch = :epoch.
-- Barreira (último comando de toda transação de escrita, lock só até o commit):
--   UPDATE ohip_lease SET last_write_at = SYS_EXTRACT_UTC(SYSTIMESTAMP)
--    WHERE lease_name = :n AND epoch = :epoch      -- 0 linhas => ROLLBACK
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_lease (
    lease_name          VARCHAR2(40)    NOT NULL,
    epoch               NUMBER(19)      DEFAULT 0 NOT NULL,
    owner               VARCHAR2(100),                      -- host:pid:uuid da instância
    acquired_at         TIMESTAMP(6),
    expires_at          TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    last_write_at       TIMESTAMP(6),
    CONSTRAINT ohip_lease_pk PRIMARY KEY (lease_name)
);

-- -----------------------------------------------------------------------------
-- Outbox transacional: mensagens a publicar no RabbitMQ.
-- Ordem de publicação por chain = ordem de id.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_outbox (
    id                  NUMBER(19)      NOT NULL,
    event_raw_id        NUMBER(19)      NOT NULL,
    chain_code          VARCHAR2(20)    NOT NULL,
    unique_event_id     VARCHAR2(64)    NOT NULL,
    exchange_name       VARCHAR2(100)   DEFAULT 'ohip.events' NOT NULL,   -- 'ohip.reprocess' para reprocessamentos
    routing_key         VARCHAR2(255)   NOT NULL,
    message             CLOB            NOT NULL,           -- contrato JSON v1 (docs/ARCHITECTURE.md §6)
    schema_version      NUMBER(3)       NOT NULL,
    status              VARCHAR2(10)    DEFAULT 'PENDING' NOT NULL,
    attempts            NUMBER(5)       DEFAULT 0 NOT NULL,
    next_attempt_at     TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    last_error          VARCHAR2(4000),
    created_at          TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    sent_at             TIMESTAMP(6),
    CONSTRAINT ohip_outbox_pk PRIMARY KEY (id),
    CONSTRAINT ohip_outbox_fk_raw FOREIGN KEY (event_raw_id) REFERENCES ohip_event_raw (id),
    CONSTRAINT ohip_outbox_ck_status CHECK (status IN ('PENDING', 'SENT', 'FAILED'))
);

CREATE INDEX ohip_outbox_ix_head     ON ohip_outbox (chain_code, status, id);  -- cabeça da fila por chain
CREATE INDEX ohip_outbox_ix_raw      ON ohip_outbox (event_raw_id);
CREATE INDEX ohip_outbox_ix_sent     ON ohip_outbox (sent_at);   -- expurgo

-- -----------------------------------------------------------------------------
-- DLQ própria (o OHIP não tem DLQ no servidor).
-- stage CONSUME: mensagem que nem virou evento (sem event_raw_id; texto em raw_message).
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_dlq (
    id                  NUMBER(19)      NOT NULL,
    stage               VARCHAR2(10)    NOT NULL,
    chain_code          VARCHAR2(20),
    event_raw_id        NUMBER(19),
    outbox_id           NUMBER(19),
    unique_event_id     VARCHAR2(64),
    offset_value        VARCHAR2(20),
    raw_message         CLOB,
    error_class         VARCHAR2(200)   NOT NULL,
    error_message       VARCHAR2(4000)  NOT NULL,
    stack_trace         CLOB,
    code_version        VARCHAR2(40),                       -- commit do código que falhou
    attempts            NUMBER(5)       DEFAULT 0 NOT NULL,
    created_at          TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    retry_requested_at  TIMESTAMP(6),                       -- CONSUME: pedido pela API, executado pelo consumer
    retry_requested_by  VARCHAR2(100),
    resolved_at         TIMESTAMP(6),
    resolved_by         VARCHAR2(100),
    resolution          VARCHAR2(20),
    CONSTRAINT ohip_dlq_pk PRIMARY KEY (id),
    CONSTRAINT ohip_dlq_fk_raw FOREIGN KEY (event_raw_id) REFERENCES ohip_event_raw (id),
    CONSTRAINT ohip_dlq_fk_outbox FOREIGN KEY (outbox_id) REFERENCES ohip_outbox (id),
    CONSTRAINT ohip_dlq_ck_stage CHECK (stage IN ('CONSUME', 'PUBLISH', 'NORMALIZE', 'ENRICH')),
    CONSTRAINT ohip_dlq_ck_resolution CHECK (
        resolution IS NULL OR resolution IN ('RETRIED', 'DISCARDED')
    )
);

CREATE INDEX ohip_dlq_ix_open  ON ohip_dlq (resolved_at, stage);
CREATE INDEX ohip_dlq_ix_raw   ON ohip_dlq (event_raw_id);
CREATE INDEX ohip_dlq_ix_obx   ON ohip_dlq (outbox_id);

-- -----------------------------------------------------------------------------
-- Estado da conexão por chain (lido pela API e pelo painel).
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_consumer_status (
    chain_code          VARCHAR2(20)    NOT NULL,
    state               VARCHAR2(20)    NOT NULL,
    instance_id         VARCHAR2(100),
    subscription_id     VARCHAR2(36),
    connected_at        TIMESTAMP(6),
    last_ping_at        TIMESTAMP(6),
    last_pong_at        TIMESTAMP(6),
    last_message_at     TIMESTAMP(6),
    last_rtt_ms         NUMBER(10),                         -- RTT suavizado do heartbeat
    reconnects          NUMBER(10)      DEFAULT 0 NOT NULL,   -- acumulado
    consecutive_failures NUMBER(5)      DEFAULT 0 NOT NULL,   -- zera ao assinar com sucesso
    last_close_code     NUMBER(5),
    last_close_reason   VARCHAR2(500),
    last_disconnect_at  TIMESTAMP(6),                       -- relógio do banco; regra dos 10 s após crash/restart
    next_attempt_at     TIMESTAMP(6),
    token_expires_at    TIMESTAMP(6),
    updated_at          TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    CONSTRAINT ohip_consumer_status_pk PRIMARY KEY (chain_code),
    CONSTRAINT ohip_cons_status_ck_state CHECK (state IN (
        'ACQUIRING', 'CONNECTING', 'INIT_SENT', 'STATUS_CHECK', 'SUBSCRIBED',
        'DRAINING', 'WAITING', 'STOPPED'
    ))
);

-- -----------------------------------------------------------------------------
-- Pedidos de replay (gravados pela API, aplicados pelo consumer na próxima
-- reconexão controlada). Tabela no Oracle para ter auditoria e durabilidade.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_replay_request (
    id                  NUMBER(19)      NOT NULL,
    chain_code          VARCHAR2(20)    NOT NULL,
    from_offset         VARCHAR2(20)    NOT NULL,
    reason              VARCHAR2(500)   NOT NULL,
    requested_by        VARCHAR2(100)   NOT NULL,
    status              VARCHAR2(10)    DEFAULT 'PENDING' NOT NULL,
    error_message       VARCHAR2(1000),
    created_at          TIMESTAMP(6)    DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    applied_at          TIMESTAMP(6),
    CONSTRAINT ohip_replay_request_pk PRIMARY KEY (id),
    CONSTRAINT ohip_replay_ck_status CHECK (status IN ('PENDING', 'APPLIED', 'REJECTED', 'CANCELLED'))
);

CREATE INDEX ohip_replay_ix_chain ON ohip_replay_request (chain_code, created_at);

-- No máximo um pedido PENDING por chain (índice único por função, válido no 11g).
CREATE UNIQUE INDEX ohip_replay_ux_pending ON ohip_replay_request (
    CASE WHEN status = 'PENDING' THEN chain_code END
);
