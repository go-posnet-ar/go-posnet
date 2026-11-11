-- Exige volver antes a una versión del servicio que no use las columnas. Un lote cuyos totales
-- cambiaron después de enviarse vuelve a reusar la confirmación anterior.

BEGIN;

ALTER TABLE settlement.batch_submissions
    DROP COLUMN IF EXISTS total_count,
    DROP COLUMN IF EXISTS total_amount,
    DROP COLUMN IF EXISTS adjustment_count,
    DROP COLUMN IF EXISTS adjustment_amount;

COMMIT;
