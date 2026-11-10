-- Exige volver antes a una versión del servicio que no use la tabla. Se pierden los totales de los
-- cierres sin lote que todavía no se usaron: el cierre automático cerrará esos lotes sin conciliar.

BEGIN;

DROP TABLE IF EXISTS settlement.pending_terminal_closes;

COMMIT;
