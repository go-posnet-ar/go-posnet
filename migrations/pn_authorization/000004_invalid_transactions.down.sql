-- Borra el registro de las transacciones rechazadas por datos inválidos, que no se puede
-- reconstruir. Exige volver antes a la versión anterior del servicio.

DROP TABLE IF EXISTS pn_authorization.invalid_transactions;
