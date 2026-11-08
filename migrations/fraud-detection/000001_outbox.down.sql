-- Borra el outbox. Exige volver antes a la versión anterior del servicio, y falla sin cambiar
-- nada si quedan scores sin publicar: borrarlos dejaría ventas esperando su respuesta.

BEGIN;

DO $$
BEGIN
    IF to_regclass('fraud_detection.outbox') IS NOT NULL
       AND EXISTS (SELECT 1 FROM fraud_detection.outbox) THEN
        RAISE EXCEPTION 'fraud_detection.outbox tiene scores sin publicar: esperá a que el relay los publique';
    END IF;
END $$;

DROP TABLE IF EXISTS fraud_detection.outbox;

COMMIT;
