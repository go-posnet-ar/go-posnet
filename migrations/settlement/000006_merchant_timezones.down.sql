-- Revierte las zonas horarias por comercio.
--
-- ATENCIÓN: desplegar antes la versión anterior del servicio, que no usa esta tabla. Los
-- comercios configurados vuelven a la zona global: sus lotes abiertos pueden quedar con otra
-- fecha que las ventas siguientes.

BEGIN;

DROP TABLE settlement.merchant_timezones;

COMMIT;
