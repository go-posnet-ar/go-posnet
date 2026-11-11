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
    version         INTEGER     NOT NULL DEFAULT 0,

    -- Contra qué se concilió el cierre (TERMINAL o BACKEND) y, con TERMINAL, sus totales.
    -- Migración 000007.
    reconciled_with VARCHAR(10),
    terminal_count  INTEGER,
    terminal_amount BIGINT,

    -- Ajustes de lotes ya enviados: cuántos y su suma, con signo. total_amount los incluye;
    -- total_count no. Migración 000009.
    adjustment_count  INTEGER,
    adjustment_amount BIGINT
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
    tx_type         VARCHAR(20) NOT NULL,        -- PURCHASE | REVERSAL | ADJUSTMENT (con signo)
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

-- ─── batch_audit_log (cambios de estado hechos por operadores) ───────────────

CREATE TABLE IF NOT EXISTS settlement.batch_audit_log (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id               UUID        NOT NULL REFERENCES settlement.settlement_batches(id),
    action                 VARCHAR(30) NOT NULL,
    operator_id            TEXT        NOT NULL,
    reason                 TEXT,
    from_state             VARCHAR(20) NOT NULL,
    to_state               VARCHAR(20) NOT NULL,
    previous_discrepancies INTEGER     NOT NULL,
    total_amount           BIGINT,
    confirmation_id        TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_batch_audit_log_batch ON settlement.batch_audit_log (batch_id, created_at);
-- Solo admite altas (000015): UPDATE, DELETE y TRUNCATE fallan.
CREATE OR REPLACE FUNCTION settlement.reject_audit_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'settlement.batch_audit_log is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;
DROP TRIGGER IF EXISTS batch_audit_log_no_update ON settlement.batch_audit_log;
CREATE TRIGGER batch_audit_log_no_update
    BEFORE UPDATE OR DELETE ON settlement.batch_audit_log
    FOR EACH ROW EXECUTE FUNCTION settlement.reject_audit_change();
DROP TRIGGER IF EXISTS batch_audit_log_no_truncate ON settlement.batch_audit_log;
CREATE TRIGGER batch_audit_log_no_truncate
    BEFORE TRUNCATE ON settlement.batch_audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION settlement.reject_audit_change();

-- ─── reversal_reviews (reversas que no coinciden con su compra) ──────────────

CREATE TABLE IF NOT EXISTS settlement.reversal_reviews (
    transaction_id       UUID        PRIMARY KEY,
    batch_id             UUID        NOT NULL REFERENCES settlement.settlement_batches(id),
    reason               VARCHAR(30) NOT NULL,
    merchant_id          UUID        NOT NULL,
    reversal_amount      BIGINT      NOT NULL,
    reversal_currency    CHAR(3)     NOT NULL,
    purchase_merchant_id UUID,
    purchase_amount      BIGINT,
    purchase_currency    CHAR(3),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at          TIMESTAMPTZ,
    resolved_by          TEXT,
    resolution           TEXT,
    action               VARCHAR(30) -- Acción del operador. Migración 000007
);
CREATE INDEX IF NOT EXISTS idx_reversal_reviews_open ON settlement.reversal_reviews (created_at)
    WHERE resolved_at IS NULL;

-- ─── merchant_timezones (zona del día de negocio, por comercio) ──────────────

CREATE TABLE IF NOT EXISTS settlement.merchant_timezones (
    merchant_id UUID        PRIMARY KEY,
    timezone    TEXT        NOT NULL,
    updated_by  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─── maintenance_runs (tareas a ejecutar una vez por entorno) ────────────────
-- Migración 000008. 01-init.sql inserta 'reversal-recovery': una base nueva no tiene reversas
-- anteriores sin descontar.

CREATE TABLE IF NOT EXISTS settlement.maintenance_runs (
    name         TEXT        PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_by TEXT        NOT NULL,
    details      TEXT
);

-- ─── batch_submissions (envíos al procesador) ───────────────────────────────────
-- Migración 000011. Un intento se registra antes de llamar al procesador y la confirmación al
-- recibirla: confirmation_id NULL es un intento sin resultado conocido.

CREATE TABLE IF NOT EXISTS settlement.batch_submissions (
    batch_id         UUID        PRIMARY KEY REFERENCES settlement.settlement_batches (id),
    attempts         INT         NOT NULL,
    first_attempt_at TIMESTAMPTZ NOT NULL,
    last_attempt_at  TIMESTAMPTZ NOT NULL,
    confirmation_id  TEXT,
    confirmed_at     TIMESTAMPTZ,
    -- Totales del primer intento (000013): otro intento con totales distintos no se envía.
    total_count       INT,
    total_amount      BIGINT,
    adjustment_count  INT,
    adjustment_amount BIGINT
);

-- ─── pending_terminal_closes (cierres del terminal sin lote) ──────────────────────
-- Migración 000012. Totales de un cierre sin ningún lote del día; los consume el cierre automático
-- del lote que se forme después, o un cierre posterior del terminal.

CREATE TABLE IF NOT EXISTS settlement.pending_terminal_closes (
    terminal_id     UUID        NOT NULL,
    batch_date      DATE        NOT NULL,
    currency        CHAR(3)     NOT NULL,
    terminal_count  INT         NOT NULL,
    terminal_amount BIGINT      NOT NULL,
    event_id        TEXT        NOT NULL,
    requested_at    TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (terminal_id, batch_date, currency)
);

-- ─── settlement_completions (revisiones de settlement.completed) ──────────────────
-- Migración 000016. Una fila por resumen emitido: numera el siguiente y calcula su diferencia.

CREATE TABLE IF NOT EXISTS settlement.settlement_completions (
    merchant_id       UUID        NOT NULL,
    settlement_date   DATE        NOT NULL,
    currency          CHAR(3)     NOT NULL,
    revision          INT         NOT NULL,
    total_batches     INT         NOT NULL,
    total_amount      BIGINT      NOT NULL,
    adjustment_amount BIGINT      NOT NULL,
    net_amount        BIGINT,
    event_id          TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, settlement_date, currency, revision)
);
