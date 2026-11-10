-- Exige volver antes a una versión del servicio que no use estas columnas. Se pierde la
-- referencia de conciliación de los lotes y la acción de las revisiones cerradas.

BEGIN;

ALTER TABLE settlement.reversal_reviews DROP COLUMN IF EXISTS action;

ALTER TABLE settlement.settlement_batches
    DROP COLUMN IF EXISTS terminal_amount,
    DROP COLUMN IF EXISTS terminal_count,
    DROP COLUMN IF EXISTS reconciled_with;

COMMIT;
