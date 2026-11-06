-- Revisión de las reversas que no coinciden con su compra.
--
-- ATENCIÓN: cada venta y cada reversa usan esta tabla. Sin ella, fallan todas y, al agotar
-- los reintentos, sus eventos se pierden. Aplicar antes de desplegar.

BEGIN;

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
    resolution           TEXT
);
CREATE INDEX IF NOT EXISTS idx_reversal_reviews_open ON settlement.reversal_reviews (created_at)
    WHERE resolved_at IS NULL;

COMMIT;
