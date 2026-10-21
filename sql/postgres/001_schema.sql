-- =============================================================================
-- 001_schema.sql — Sequências e tabelas do consumidor OHIP Streaming
-- Alvo: PostgreSQL 15+ (ADR-0021). Tradução de sql/001 e sql/002 (Oracle 11g):
-- mesmas tabelas, colunas e nomes de constraints e índices (contrato, RNF-15).
-- Datas em timestamptz (UTC). Payload e mensagens em text, como recebidos.
-- RASCUNHO DA FASE 11 — aplicado pelo DBA, como ohip_owner com search_path=ohip
-- (900_grants_example.sql): psql -v ON_ERROR_STOP=1 --single-transaction -f ...
-- O projeto só roda este script no contêiner descartável criado por make test-db
-- (ADR-0021 §9).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Sequências explícitas: a aplicação reserva ids antes do INSERT (reserve_event_ids).
-- No PostgreSQL o cache de sequência é por sessão: OHIP_OUTBOX_SEQ fica com CACHE 1
-- para os ids da outbox seguirem a ordem de gravação entre sessões (ADR-0021 §4).
-- -----------------------------------------------------------------------------
CREATE SEQUENCE ohip_event_raw_seq  AS bigint START WITH 1 INCREMENT BY 1 CACHE 100 NO CYCLE;
CREATE SEQUENCE ohip_outbox_seq     AS bigint START WITH 1 INCREMENT BY 1 CACHE 1   NO CYCLE;
CREATE SEQUENCE ohip_dlq_seq        AS bigint START WITH 1 INCREMENT BY 1 CACHE 1   NO CYCLE;
CREATE SEQUENCE ohip_replay_req_seq AS bigint START WITH 1 INCREMENT BY 1 CACHE 1   NO CYCLE;

-- -----------------------------------------------------------------------------
-- Evento bruto: fonte da verdade. Uma linha por uniqueEventId.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_event_raw (
    id                  bigint          NOT NULL,
    chain_code          varchar(20)     NOT NULL,
    hotel_id            varchar(50),                        -- pode vir nulo (ex.: perfis compartilhados)
    offset_value        varchar(20)     NOT NULL,           -- string ^[0-9]+$ (schema OHIP); nunca tratar como número no protocolo
    unique_event_id     varchar(64)     NOT NULL,
    subscription_id     varchar(36),                        -- GUID do subscribe que entregou o evento
    module_name         varchar(100)    NOT NULL,
    event_name          varchar(100)    NOT NULL,
    primary_key         varchar(100)    NOT NULL,
    publisher_id        varchar(50),
    action_instance_id  varchar(50),
    event_ts            timestamptz(3),                     -- campo "timestamp" do OHIP; fuso a confirmar (D-3)
    payload             text            NOT NULL,           -- JSON do newEvent, como recebido (text, não jsonb: ADR-0021 §4)
    processing_status   varchar(20)     NOT NULL DEFAULT 'RECEIVED',
    received_at         timestamptz     NOT NULL,           -- chegada no consumer
    persisted_at        timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz,
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
-- Último offset confirmado por chain (atualizado na transação do lote).
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_offset (
    chain_code              varchar(20)     NOT NULL,
    last_offset             varchar(20),
    last_unique_event_id    varchar(64),
    updated_at              timestamptz     NOT NULL DEFAULT now(),
    CONSTRAINT ohip_offset_pk PRIMARY KEY (chain_code)
);

-- -----------------------------------------------------------------------------
-- Lease com prazo e epoch (ADR-0008): consumer:<chain> e publisher.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_lease (
    lease_name          varchar(40)     NOT NULL,
    epoch               bigint          NOT NULL DEFAULT 0,
    owner               varchar(100),                       -- host:pid:uuid da instância
    acquired_at         timestamptz,
    expires_at          timestamptz     NOT NULL DEFAULT now(),
    last_write_at       timestamptz,
    CONSTRAINT ohip_lease_pk PRIMARY KEY (lease_name)
);

-- -----------------------------------------------------------------------------
-- Outbox transacional (ADR-0002).
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_outbox (
    id                  bigint          NOT NULL,
    event_raw_id        bigint          NOT NULL,
    chain_code          varchar(20)     NOT NULL,
    unique_event_id     varchar(64)     NOT NULL,
    exchange_name       varchar(100)    NOT NULL DEFAULT 'ohip.events',   -- 'ohip.reprocess' para reprocessamentos
    routing_key         varchar(255)    NOT NULL,
    message             text            NOT NULL,           -- contrato JSON v1 (docs/ARCHITECTURE.md §6)
    schema_version      smallint        NOT NULL,
    status              varchar(10)     NOT NULL DEFAULT 'PENDING',
    attempts            integer         NOT NULL DEFAULT 0,
    next_attempt_at     timestamptz     NOT NULL DEFAULT now(),
    last_error          varchar(4000),
    created_at          timestamptz     NOT NULL DEFAULT now(),
    sent_at             timestamptz,
    CONSTRAINT ohip_outbox_pk PRIMARY KEY (id),
    CONSTRAINT ohip_outbox_fk_raw FOREIGN KEY (event_raw_id) REFERENCES ohip_event_raw (id),
    CONSTRAINT ohip_outbox_ck_status CHECK (status IN ('PENDING', 'SENT', 'FAILED'))
);

CREATE INDEX ohip_outbox_ix_head     ON ohip_outbox (chain_code, status, id);  -- cabeça da fila por chain
CREATE INDEX ohip_outbox_ix_raw      ON ohip_outbox (event_raw_id);
CREATE INDEX ohip_outbox_ix_sent     ON ohip_outbox (sent_at);   -- expurgo

-- -----------------------------------------------------------------------------
-- DLQ por etapa (RF-07, ADR-0009).
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_dlq (
    id                  bigint          NOT NULL,
    stage               varchar(10)     NOT NULL,
    chain_code          varchar(20),
    event_raw_id        bigint,
    outbox_id           bigint,
    unique_event_id     varchar(64),
    offset_value        varchar(20),
    raw_message         text,                               -- NUL trocado por U+FFFD (ADR-0021 §6)
    error_class         varchar(200)    NOT NULL,
    error_message       varchar(4000)   NOT NULL,
    stack_trace         text,
    code_version        varchar(40),                        -- commit do código que falhou
    attempts            integer         NOT NULL DEFAULT 0,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    retry_requested_at  timestamptz,                        -- CONSUME: pedido pela API, executado pelo consumer
    retry_requested_by  varchar(100),
    resolved_at         timestamptz,
    resolved_by         varchar(100),
    resolution          varchar(20),
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
-- Status do consumer por chain (informativo; regra dos 10 s via last_disconnect_at).
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_consumer_status (
    chain_code           varchar(20)    NOT NULL,
    state                varchar(20)    NOT NULL,
    instance_id          varchar(100),
    subscription_id      varchar(36),
    connected_at         timestamptz,
    last_ping_at         timestamptz,
    last_pong_at         timestamptz,
    last_message_at      timestamptz,
    last_rtt_ms          integer,                           -- RTT suavizado do heartbeat
    reconnects           integer        NOT NULL DEFAULT 0, -- acumulado
    consecutive_failures integer        NOT NULL DEFAULT 0, -- zera após sessão saudável (ADR-0020)
    last_close_code      integer,
    last_close_reason    varchar(500),
    last_disconnect_at   timestamptz,                       -- relógio do banco; regra dos 10 s após crash/restart
    next_attempt_at      timestamptz,
    token_expires_at     timestamptz,
    updated_at           timestamptz    NOT NULL DEFAULT now(),
    CONSTRAINT ohip_consumer_status_pk PRIMARY KEY (chain_code),
    CONSTRAINT ohip_cons_status_ck_state CHECK (state IN (
        'ACQUIRING', 'CONNECTING', 'INIT_SENT', 'STATUS_CHECK', 'SUBSCRIBED',
        'DRAINING', 'WAITING', 'STOPPED'
    ))
);

-- -----------------------------------------------------------------------------
-- Pedidos de replay (DV-8, ARCHITECTURE §4.6). No máximo um PENDING por chain.
-- -----------------------------------------------------------------------------
CREATE TABLE ohip_replay_request (
    id                  bigint          NOT NULL,
    chain_code          varchar(20)     NOT NULL,
    from_offset         varchar(20)     NOT NULL,
    reason              varchar(500)    NOT NULL,
    requested_by        varchar(100)    NOT NULL,
    status              varchar(10)     NOT NULL DEFAULT 'PENDING',
    error_message       varchar(1000),
    created_at          timestamptz     NOT NULL DEFAULT now(),
    applied_at          timestamptz,
    cancelled_at        timestamptz,
    cancelled_by        varchar(100),
    CONSTRAINT ohip_replay_request_pk PRIMARY KEY (id),
    CONSTRAINT ohip_replay_ck_status CHECK (status IN ('PENDING', 'APPLIED', 'REJECTED', 'CANCELLED'))
);

CREATE INDEX ohip_replay_ix_chain ON ohip_replay_request (chain_code, created_at);

-- Índice único parcial (no Oracle era índice por função com CASE).
CREATE UNIQUE INDEX ohip_replay_ux_pending ON ohip_replay_request (chain_code)
    WHERE status = 'PENDING';
