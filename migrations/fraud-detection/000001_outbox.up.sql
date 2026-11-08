-- Outbox de Fraud Detection: cada posnet.fraud.score-calculated se escribe acá, en la misma
-- transacción que guarda el caso y marca el evento como procesado, y un relay lo publica a
-- JetStream. Si NATS está caído o el proceso muere, el score no se pierde: sale cuando se
-- recupera.
--
-- ATENCIÓN: el servicio verifica esta tabla al arrancar y, sin ella, no arranca. Aplicar antes de
-- desplegar. Se puede aplicar con el servicio anterior en marcha.

BEGIN;

CREATE TABLE IF NOT EXISTS fraud_detection.outbox (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    subject    TEXT        NOT NULL,
    event_id   TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fd_outbox_created_at ON fraud_detection.outbox (created_at);

COMMIT;
