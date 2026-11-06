-- Revierte la revisión de reversas.
--
-- ATENCIÓN: desplegar antes la versión anterior del servicio, que no usa esta tabla. Falla
-- si hay revisiones abiertas, porque se perderían. Si falla, no cambia nada.

BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM settlement.reversal_reviews WHERE resolved_at IS NULL) THEN
        RAISE EXCEPTION 'settlement.reversal_reviews tiene revisiones abiertas';
    END IF;
END $$;

DROP TABLE settlement.reversal_reviews;

COMMIT;
