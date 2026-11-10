-- Envíos de lotes al procesador. Settlement registra cada intento antes de llamar al procesador, y
-- la confirmación apenas la recibe, aparte del lote: si después falla guardar el lote en
-- SUBMITTED, un resubmit encuentra la confirmación y no lo envía otra vez.
--
-- confirmation_id NULL con attempts > 0 es un intento sin resultado conocido: falló o se cortó.
-- Se repite con el mismo batch_id, que el procesador usa como clave de idempotencia.
--
-- Completa los envíos que hizo resubmit, que guardaba la confirmación en batch_audit_log. Los del
-- cierre no la guardaban en ningún lado: esos lotes quedan sin fila.
--
-- ATENCIÓN: /readyz responde 503 sin esta tabla. Se puede aplicar con el servicio anterior en
-- marcha: no la usa.

BEGIN;

CREATE TABLE IF NOT EXISTS settlement.batch_submissions (
    batch_id         UUID        PRIMARY KEY REFERENCES settlement.settlement_batches (id),
    attempts         INT         NOT NULL,
    first_attempt_at TIMESTAMPTZ NOT NULL,
    last_attempt_at  TIMESTAMPTZ NOT NULL,
    confirmation_id  TEXT,
    confirmed_at     TIMESTAMPTZ
);

INSERT INTO settlement.batch_submissions
    (batch_id, attempts, first_attempt_at, last_attempt_at, confirmation_id, confirmed_at)
SELECT DISTINCT ON (a.batch_id) a.batch_id, 1, a.created_at, a.created_at, a.confirmation_id, a.created_at
FROM settlement.batch_audit_log a
JOIN settlement.settlement_batches b ON b.id = a.batch_id
WHERE a.action = 'RESUBMIT' AND a.confirmation_id IS NOT NULL AND a.confirmation_id <> ''
  AND b.state IN ('SUBMITTED', 'SETTLED')
ORDER BY a.batch_id, a.created_at DESC
ON CONFLICT (batch_id) DO NOTHING;

COMMIT;
