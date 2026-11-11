-- Vuelve a permitir modificar y borrar la auditoría. El servicio no lo necesita: no hace falta
-- revertir esta migración para volver a una versión anterior.

BEGIN;

DROP TRIGGER IF EXISTS batch_audit_log_no_update ON settlement.batch_audit_log;
DROP TRIGGER IF EXISTS batch_audit_log_no_truncate ON settlement.batch_audit_log;
DROP FUNCTION IF EXISTS settlement.reject_audit_change();

COMMIT;
