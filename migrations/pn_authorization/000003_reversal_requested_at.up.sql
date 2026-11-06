-- Momento en que se pidió la reversa de una transacción. Mientras siga en APPROVED con este
-- dato, el conciliador la reintenta: un pedido de reversa ya no se pierde si falla el adquirente.
--
-- ATENCIÓN: cada lectura de una transacción usa esta columna. Sin ella, fallan todas las
-- ventas. /readyz responde 503 y Docker marca el contenedor unhealthy. Aplicar antes de desplegar.

BEGIN;

ALTER TABLE pn_authorization.transactions
    ADD COLUMN IF NOT EXISTS reversal_requested_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_auth_txn_reversal_pending
    ON pn_authorization.transactions (reversal_requested_at)
    WHERE state = 'APPROVED' AND reversal_requested_at IS NOT NULL;

COMMIT;
