-- Ajustes de lotes ya enviados. Corregir una reversa de un lote SUBMITTED o SETTLED agrega una
-- fila ADJUSTMENT, con signo, en el lote abierto del mismo terminal: el dinero se corrige en la
-- liquidación siguiente. Estas columnas guardan cuántos ajustes tiene el lote y su suma.
-- total_amount los incluye; total_count no, y la conciliación con el terminal los deja afuera.
--
-- batch_transactions no cambia: tx_type ya admite cualquier valor, y amount_cents, el signo.
--
-- ATENCIÓN: sin estas columnas falla cada guardado de un lote, y /readyz responde 503 con las
-- columnas que faltan. Aplicar antes de desplegar. Se puede aplicar con el servicio anterior en
-- marcha: las columnas nuevas admiten NULL.

BEGIN;

ALTER TABLE settlement.settlement_batches
    ADD COLUMN IF NOT EXISTS adjustment_count  INTEGER,
    ADD COLUMN IF NOT EXISTS adjustment_amount BIGINT;

COMMIT;
