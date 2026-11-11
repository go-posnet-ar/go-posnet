-- Exige volver antes a una versión del servicio que no use la tabla. Los resúmenes vuelven a salir
-- sin revisión, y la liquidación tardía de una moneda repite el de las demás.

BEGIN;

DROP TABLE IF EXISTS settlement.settlement_completions;

COMMIT;
