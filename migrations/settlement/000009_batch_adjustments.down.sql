-- Exige volver antes a una versión del servicio que no use estas columnas, y falla sin cambiar
-- nada si hay ajustes registrados: la versión anterior los sumaría como compras.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM settlement.batch_transactions WHERE tx_type = 'ADJUSTMENT') THEN
        RAISE EXCEPTION 'settlement.batch_transactions tiene ajustes: la versión anterior no los entiende';
    END IF;
END $$;

ALTER TABLE settlement.settlement_batches
    DROP COLUMN IF EXISTS adjustment_amount,
    DROP COLUMN IF EXISTS adjustment_count;

COMMIT;
