-- Revierte la auditoría de lotes.
--
-- ATENCIÓN: falla si la tabla tiene registros, porque se perdería la auditoría. Si falla,
-- no cambia nada: exportarlos y vaciarla es una decisión que requiere aprobación.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM settlement.batch_audit_log) THEN
        RAISE EXCEPTION 'settlement.batch_audit_log tiene registros';
    END IF;
END $$;

DROP TABLE settlement.batch_audit_log;

COMMIT;
