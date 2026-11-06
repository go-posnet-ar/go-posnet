-- Outbox de los eventos batch-closed y settlement.completed.
--
-- El servicio guarda el evento en la misma transacción que el lote: sin esta tabla,
-- fallan todos los cierres y las liquidaciones. Aplicar antes de desplegar.

BEGIN;

CREATE TABLE IF NOT EXISTS settlement.outbox (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    subject    TEXT        NOT NULL,
    event_id   TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_settlement_outbox_created_at ON settlement.outbox (created_at);

COMMIT;
