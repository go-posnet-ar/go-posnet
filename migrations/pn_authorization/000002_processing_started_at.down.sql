-- Revierte processing_started_at.
--
-- ATENCIÓN: desplegar antes la versión anterior del servicio, que no usa la columna. Falla si
-- hay transacciones sin resolver: sin el conciliador, nadie las resolvería. Si falla, no cambia nada.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pn_authorization.transactions WHERE state IN ('PROCESSING', 'INDETERMINATE')) THEN
        RAISE EXCEPTION 'pn_authorization.transactions tiene transacciones en PROCESSING o INDETERMINATE';
    END IF;
END $$;

DROP INDEX IF EXISTS pn_authorization.idx_auth_txn_unresolved;
ALTER TABLE pn_authorization.transactions DROP COLUMN IF EXISTS processing_started_at;

COMMIT;
