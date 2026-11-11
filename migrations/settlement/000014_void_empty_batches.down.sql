-- Exige volver antes a una versión del servicio que no conozca VOID: la anterior no puede leer esos
-- lotes. Los devuelve a CLOSED, donde quedan sin salida, como antes. La auditoría de 000014 queda.

BEGIN;

UPDATE settlement.settlement_batches
SET state = 'CLOSED', version = version + 1
WHERE state = 'VOID';

COMMIT;
