-- Liberar los lotes retenidos por una revisión de reversa, y restituir una reversa excluida.
--
-- settlement_batches guarda contra qué se concilió el cierre (reconciled_with) y, si fue contra
-- el terminal, los totales que informó. Al cerrarse la última revisión del lote, se concilia de
-- nuevo: si coincide, el lote pasa a CLOSED y se envía. Los lotes cerrados antes de esta
-- migración quedan sin referencia y no se liberan solos.
--
-- reversal_reviews guarda la acción del operador. Con EXCLUDE, si la compra llega después, la
-- reversa se restituye o la revisión se reabre.
--
-- ATENCIÓN: sin estas columnas falla cada guardado de un lote, y /readyz responde 503 con las
-- columnas que faltan. Aplicar antes de desplegar. Se puede aplicar con el servicio anterior en
-- marcha: las columnas nuevas admiten NULL.

BEGIN;

ALTER TABLE settlement.settlement_batches
    ADD COLUMN IF NOT EXISTS reconciled_with VARCHAR(10),
    ADD COLUMN IF NOT EXISTS terminal_count  INTEGER,
    ADD COLUMN IF NOT EXISTS terminal_amount BIGINT;

ALTER TABLE settlement.reversal_reviews
    ADD COLUMN IF NOT EXISTS action VARCHAR(30);

COMMIT;
