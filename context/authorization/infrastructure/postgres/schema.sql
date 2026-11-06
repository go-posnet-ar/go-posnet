-- Schema del BC Authorization
-- Ejecutado por golang-migrate al arrancar el servicio.

CREATE SCHEMA IF NOT EXISTS authorization;

-- ─── transactions ─────────────────────────────────────────────────────────────

CREATE TABLE authorization.transactions (
    -- Identidad
    id              UUID        PRIMARY KEY,
    terminal_id     UUID        NOT NULL,
    merchant_id     UUID        NOT NULL,

    -- Estado
    state           VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',

    -- Datos financieros
    amount_cents    BIGINT      NOT NULL,
    currency        CHAR(3)     NOT NULL,

    -- Tarjeta (solo datos no sensibles)
    pan_last4       CHAR(4)     NOT NULL,
    card_network    VARCHAR(20) NOT NULL,
    entry_mode      VARCHAR(20) NOT NULL,
    stan            INTEGER     NOT NULL,

    -- Resultado
    auth_code           VARCHAR(6),
    rejection_code      VARCHAR(20),
    rejection_source    VARCHAR(20),

    -- Antifraude
    fraud_score         SMALLINT,
    fraud_decision      VARCHAR(10),
    fraud_rules_hit     JSONB,

    -- Datos EMV (opacos — reenviados al adquirente)
    emv_data_b64        TEXT,
    iso8583_raw         BYTEA,

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    authorized_at   TIMESTAMPTZ,
    rejected_at     TIMESTAMPTZ,
    processing_started_at TIMESTAMPTZ,
    reversal_requested_at TIMESTAMPTZ,

    -- FK a settlement cuando la transacción es liquidada
    settlement_batch_id UUID
);

-- Índices para queries frecuentes
CREATE INDEX idx_auth_txn_terminal_date
    ON authorization.transactions(terminal_id, created_at DESC);

CREATE INDEX idx_auth_txn_state
    ON authorization.transactions(state)
    WHERE state IN ('RECEIVED', 'FRAUD_CHECKING', 'PROCESSING', 'INDETERMINATE');

CREATE INDEX idx_auth_txn_unresolved
    ON authorization.transactions (created_at)
    WHERE state IN ('PROCESSING', 'INDETERMINATE');

CREATE INDEX idx_auth_txn_reversal_pending
    ON authorization.transactions (reversal_requested_at)
    WHERE state = 'APPROVED' AND reversal_requested_at IS NOT NULL;

CREATE INDEX idx_auth_txn_stan
    ON authorization.transactions(terminal_id, stan, created_at::date);

-- ─── processed_events (idempotencia) ─────────────────────────────────────────

CREATE TABLE authorization.processed_events (
    event_id        UUID        PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partitioning por mes para facilitar purga de datos históricos
-- (se puede agregar en el futuro sin cambiar la interfaz del repositorio)
