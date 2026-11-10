-- Exige volver antes a una versión del servicio que no use la tabla. Se pierde el registro de lo
-- que ya se ejecutó: con la versión nueva, la alerta volvería a pedirlo.

BEGIN;

DROP TABLE IF EXISTS settlement.maintenance_runs;

COMMIT;
