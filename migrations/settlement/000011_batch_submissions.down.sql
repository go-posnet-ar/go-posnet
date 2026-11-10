-- Exige volver antes a una versión del servicio que no use la tabla. Se pierden las confirmaciones
-- de los envíos del cierre, que solo se guardan acá.

BEGIN;

DROP TABLE IF EXISTS settlement.batch_submissions;

COMMIT;
