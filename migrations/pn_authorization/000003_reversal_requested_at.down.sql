-- Revierte reversal_requested_at.
--
-- ATENCIÓN: desplegar antes la versión anterior del servicio, que no usa la columna. Falla si hay
-- reversas pendientes: sin el conciliador, se perderían. Si falla, no cambia nada.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pn_authorization.transactions WHERE state = 'APPROVED' AND reversal_requested_at IS NOT NULL) THEN
        RAISE EXCEPTION 'pn_authorization.transactions tiene reversas pendientes';
    END IF;
END $$;

DROP INDEX IF EXISTS pn_authorization.idx_auth_txn_reversal_pending;
ALTER TABLE pn_authorization.transactions DROP COLUMN IF EXISTS reversal_requested_at;

COMMIT;
