-- Revierte el outbox de Settlement.
--
-- ATENCIÓN: falla si quedan eventos sin publicar, porque se perderían. Si falla, no
-- cambia nada: esperar a que el Relay los publique y volver a ejecutar.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM settlement.outbox) THEN
        RAISE EXCEPTION 'settlement.outbox tiene eventos sin publicar';
    END IF;
END $$;

DROP TABLE settlement.outbox;

COMMIT;
