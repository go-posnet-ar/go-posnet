-- Estado final VOID para los lotes cerrados sin transacciones. Un lote queda vacío si una revisión
-- excluye su única reversa, o si lo creó una reversa de otro día que después se excluyó. Antes, el
-- cierre lo dejaba en CLOSED para siempre: no había nada que enviar ni que liquidar. Ahora el
-- cierre, el cierre automático y la liberación lo pasan a VOID en el mismo guardado.
--
-- Pasa a VOID los lotes vacíos que ya están en CLOSED, salvo los que el procesador aceptó: esos se
-- resuelven con resubmit. Cada uno queda en la auditoría como VOID, con operator_id 'system'. Sube
-- la versión del lote: una instancia que lo tenga cargado no lo pisa.
--
-- Se puede aplicar con el servicio anterior en marcha: no lee VOID, y no toca un lote que no sea
-- CLOSED. Aplicarla antes del despliegue evita que ese servicio vea un estado que no conoce.

BEGIN;

WITH voided AS (
    UPDATE settlement.settlement_batches b
    SET state = 'VOID', version = b.version + 1
    WHERE b.state = 'CLOSED'
      AND NOT EXISTS (SELECT 1 FROM settlement.batch_transactions t WHERE t.batch_id = b.id)
      AND NOT EXISTS (SELECT 1 FROM settlement.batch_submissions s
                      WHERE s.batch_id = b.id AND s.confirmation_id IS NOT NULL)
    RETURNING b.id, b.discrepancies
)
INSERT INTO settlement.batch_audit_log
    (batch_id, action, operator_id, reason, from_state, to_state, previous_discrepancies, total_amount)
SELECT id, 'VOID', 'system', 'migration 000014: closed batch without transactions', 'CLOSED', 'VOID', discrepancies, 0
FROM voided;

COMMIT;
