-- Schema del BC Fraud Detection
-- Ejecutado por golang-migrate al arrancar el servicio.

CREATE SCHEMA IF NOT EXISTS fraud_detection;

-- ─── fraud_cases ──────────────────────────────────────────────────────────────

CREATE TABLE fraud_detection.fraud_cases (
    id              UUID        PRIMARY KEY,          -- UUID propio del FraudCase
    transaction_id  UUID        NOT NULL UNIQUE,      -- TransactionID global — correlación

    -- Input de la transacción
    terminal_id     UUID        NOT NULL,
    merchant_id     UUID        NOT NULL,
    amount_cents    BIGINT      NOT NULL,
    currency        CHAR(3)     NOT NULL,
    card_network    VARCHAR(20) NOT NULL,
    entry_mode      VARCHAR(20) NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,

    -- Resultado del análisis
    score           SMALLINT    NOT NULL,             -- 0–100
    decision        VARCHAR(10) NOT NULL,             -- APPROVE | REJECT | REVIEW
    rules_hit       JSONB       NOT NULL DEFAULT '[]', -- IDs de reglas que activaron
    evaluations     JSONB       NOT NULL DEFAULT '[]', -- Array de { rule_id, activated, score_contribution, reason }

    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fd_cases_terminal
    ON fraud_detection.fraud_cases(terminal_id, evaluated_at DESC);

CREATE INDEX idx_fd_cases_decision
    ON fraud_detection.fraud_cases(decision)
    WHERE decision IN ('REJECT', 'REVIEW');

-- ─── fraud_rules ──────────────────────────────────────────────────────────────
-- Las reglas son configurables sin redespliegue.
-- El motor las carga al arrancar y las recarga cada RulesCacheTTL.

CREATE TABLE fraud_detection.fraud_rules (
    id              VARCHAR(20) PRIMARY KEY,          -- ej: "RULE-001"
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    score_weight    SMALLINT    NOT NULL,             -- Puntos que suma al score si activa
    threshold_value NUMERIC,                          -- Umbral configurable de la regla
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed de reglas iniciales
INSERT INTO fraud_detection.fraud_rules (id, name, description, score_weight, threshold_value) VALUES
    ('RULE-001', 'Velocity Check',         'Más de 60 transacciones por hora en el terminal',              20,  60),
    ('RULE-002', 'Unusual Amount',         'Monto mayor a 3x el promedio del comercio (últimos 30 días)', 15,   3),
    ('RULE-003', 'Multiple Rejections',    'Más de 3 rechazos en los últimos 10 minutos',                 25,   3),
    ('RULE-004', 'Repeated Amount',        'Mismo monto exacto más de una vez en 5 minutos',              20,   1),
    ('RULE-005', 'High Amount Magstripe',  'Tarjeta sin chip con monto alto (> ARS 50.000)',               30,   5000000)
ON CONFLICT (id) DO NOTHING;

-- ─── processed_events (idempotencia) ─────────────────────────────────────────

CREATE TABLE fraud_detection.processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── outbox (scores por publicar) ────────────────────────────────────────────
-- Migración: migrations/fraud-detection/000001_outbox.up.sql

CREATE TABLE fraud_detection.outbox (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    subject    TEXT        NOT NULL,
    event_id   TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fd_outbox_created_at ON fraud_detection.outbox (created_at);
