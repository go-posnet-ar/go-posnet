-- Borra el outbox. Exige volver antes a la versión anterior del servicio, y falla sin cambiar
-- nada si quedan eventos sin publicar: borrarlos perdería resultados de ventas.

BEGIN;

DO $$
BEGIN
    IF to_regclass('pn_authorization.outbox') IS NOT NULL
       AND EXISTS (SELECT 1 FROM pn_authorization.outbox) THEN
        RAISE EXCEPTION 'pn_authorization.outbox tiene eventos sin publicar: esperá a que el relay los publique';
    END IF;
END $$;

DROP TABLE IF EXISTS pn_authorization.outbox;

COMMIT;
