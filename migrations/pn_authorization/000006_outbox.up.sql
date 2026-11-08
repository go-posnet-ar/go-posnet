-- Outbox de Authorization: cada evento de integración (fraud.check-requested, auth.approved,
-- auth.rejected, auth.reversal-completed) se escribe acá, en la misma transacción que el cambio
-- que lo produce, y un relay lo publica a JetStream. Si NATS está caído o el proceso muere, el
-- evento no se pierde: sale cuando se recupera.
--
-- ATENCIÓN: el servicio verifica esta tabla al arrancar y, sin ella, no arranca. Aplicar antes de
-- desplegar. Se puede aplicar con el servicio anterior en marcha.

BEGIN;

CREATE TABLE IF NOT EXISTS pn_authorization.outbox (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    subject    TEXT        NOT NULL,
    event_id   TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_auth_outbox_created_at ON pn_authorization.outbox (created_at);

COMMIT;
