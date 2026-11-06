-- Auditoría de las operaciones de administración de lotes.
--
-- El servicio escribe el registro en la misma transacción que el cambio: sin esta tabla,
-- fallan force-close, resubmit, reopen, resolve-dispute y settle. Aplicar antes de desplegar.

BEGIN;

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

COMMIT;
