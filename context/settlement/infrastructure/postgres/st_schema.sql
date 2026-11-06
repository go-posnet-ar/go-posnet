-- Schema del BC Settlement
-- Ejecutado por golang-migrate al arrancar el servicio.

CREATE SCHEMA IF NOT EXISTS settlement;

-- ─── settlement_batches ───────────────────────────────────────────────────────

CREATE TABLE settlement.settlement_batches (
    id              UUID        PRIMARY KEY,
    terminal_id     UUID        NOT NULL,
    merchant_id     UUID        NOT NULL,
    batch_date      DATE        NOT NULL,
    state           VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    currency        CHAR(3)     NOT NULL,

    -- Totales calculados al cierre (NULL mientras OPEN)
    total_count     INTEGER,
    total_amount    BIGINT,
    purchase_count  INTEGER,
    purchase_amount BIGINT,
    reversal_count  INTEGER,
    reversal_amount BIGINT,

    -- Conciliación
    discrepancies   INTEGER     NOT NULL DEFAULT 0,

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    submitted_at    TIMESTAMPTZ,
    settled_at      TIMESTAMPTZ,

    -- Bloqueo optimista
    version         INTEGER     NOT NULL DEFAULT 0
);

-- Garantiza un solo batch OPEN por terminal, día y moneda
CREATE UNIQUE INDEX uq_settlement_batches_open
    ON settlement.settlement_batches (terminal_id, batch_date, currency)
    WHERE state = 'OPEN';

CREATE INDEX idx_settlement_batches_merchant_date
    ON settlement.settlement_batches(merchant_id, batch_date DESC);

CREATE INDEX idx_settlement_batches_state
    ON settlement.settlement_batches(state)
    WHERE state IN ('OPEN', 'PENDING_CLOSE', 'CLOSED', 'SUBMITTED', 'DISPUTED');

-- ─── batch_transactions ───────────────────────────────────────────────────────

CREATE TABLE settlement.batch_transactions (
    id              UUID        PRIMARY KEY,
    batch_id        UUID        NOT NULL REFERENCES settlement.settlement_batches(id),
    transaction_id  UUID        NOT NULL,
    amount_cents    BIGINT      NOT NULL,
    currency        CHAR(3)     NOT NULL,
    tx_type         VARCHAR(20) NOT NULL,        -- PURCHASE | REVERSAL | OFFLINE
    included_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Una compra y su reversa comparten transaction_id
CREATE UNIQUE INDEX uq_batch_transactions_tx_type
    ON settlement.batch_transactions (transaction_id, tx_type);

CREATE INDEX idx_batch_transactions_batch
    ON settlement.batch_transactions(batch_id);

-- ─── processed_events (idempotencia) ─────────────────────────────────────────

CREATE TABLE settlement.processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── outbox (eventos pendientes de publicar) ─────────────────────────────────

CREATE TABLE IF NOT EXISTS settlement.outbox (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    subject    TEXT        NOT NULL,
    event_id   TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_settlement_outbox_created_at ON settlement.outbox (created_at);
