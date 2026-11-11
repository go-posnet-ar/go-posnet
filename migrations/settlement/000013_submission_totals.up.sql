-- Totales con los que se envió cada lote al procesador. Un lote que el procesador ya aceptó puede
-- quedar retenido por una revisión mientras se envía, y sus totales cambiar antes de liberarse.
-- Antes, el envío siguiente reusaba la confirmación anterior y el lote se liquidaba con totales
-- que el procesador no conocía. Ahora, si los totales difieren de los del primer intento, el lote
-- no se envía ni se marca SUBMITTED: queda CLOSED, con la alerta LoteConTotalesCambiados.
--
-- Las filas existentes toman los totales actuales de su lote: es lo mismo que suponía el código
-- anterior. Un lote que ya cambió antes de esta migración no se detecta.
--
-- ATENCIÓN: /readyz responde 503 sin estas columnas. Se puede aplicar con el servicio anterior en
-- marcha: no las usa.

BEGIN;

ALTER TABLE settlement.batch_submissions
    ADD COLUMN IF NOT EXISTS total_count       INT,
    ADD COLUMN IF NOT EXISTS total_amount      BIGINT,
    ADD COLUMN IF NOT EXISTS adjustment_count  INT,
    ADD COLUMN IF NOT EXISTS adjustment_amount BIGINT;

UPDATE settlement.batch_submissions s
SET total_count       = COALESCE(b.total_count, 0),
    total_amount      = COALESCE(b.total_amount, 0),
    adjustment_count  = COALESCE(b.adjustment_count, 0),
    adjustment_amount = COALESCE(b.adjustment_amount, 0)
FROM settlement.settlement_batches b
WHERE b.id = s.batch_id AND s.total_amount IS NULL;

COMMIT;
