-- Referencia de conciliación de los lotes retenidos antes de 000007.
--
-- Un lote retenido solo por revisiones de reversa cerró sin discrepancias: lo que informó el
-- terminal coincidía con los totales que el cierre guardó en el lote. Las acciones sobre las
-- revisiones cambian las transacciones, no esos totales, así que siguen siendo los del cierre.
-- Esta migración los copia como totales del terminal, y el job de cierre automático libera el
-- lote en su barrido siguiente si ya no tiene revisiones abiertas y sigue coincidiendo. Si una
-- revisión cambió el monto (USE_PURCHASE_AMOUNT o EXCLUDE), el lote ya no coincide: queda
-- DISPUTED con discrepancias, para un resolve-dispute o reopen.
--
-- Se concilia contra el terminal también si el lote lo cerró el job, con los totales del backend:
-- es la comparación más estricta, y en el peor caso deja el lote para un operador.
--
-- Los lotes DISPUTED con discrepancias no se tocan: no coincidieron con el terminal, y no hay
-- forma de saber qué informó.
--
-- Requiere 000007. Es idempotente: solo completa los lotes sin referencia.

BEGIN;

UPDATE settlement.settlement_batches b
SET reconciled_with = 'TERMINAL',
    terminal_count  = b.total_count,
    terminal_amount = COALESCE(b.purchase_amount, 0) - COALESCE(b.reversal_amount, 0),
    version         = b.version + 1
WHERE b.state = 'DISPUTED'
  AND b.discrepancies = 0
  AND b.reconciled_with IS NULL
  AND b.total_count IS NOT NULL
  AND EXISTS (SELECT 1 FROM settlement.reversal_reviews r WHERE r.batch_id = b.id);

COMMIT;
