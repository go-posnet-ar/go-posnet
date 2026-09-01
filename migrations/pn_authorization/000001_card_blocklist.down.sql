-- Revierte la blocklist de tarjetas y el token de tarjeta.
--
-- ATENCIÓN: DROP TABLE blocked_cards destruye el registro de las tarjetas
-- reportadas como robadas o perdidas, y ese dato no se puede reconstruir —
-- proviene de respuestas del emisor que ya pasaron. Exportar la tabla antes
-- de correr este down en cualquier entorno con datos reales.
--
-- Lo mismo aplica a transactions.card_token: el PAN nunca llega al backend,
-- así que la columna no se puede backfillear una vez borrada.

BEGIN;

DROP TABLE IF EXISTS pn_authorization.blocked_cards;

DROP INDEX IF EXISTS pn_authorization.idx_auth_txn_card_token;

ALTER TABLE pn_authorization.transactions
    DROP COLUMN IF EXISTS card_token;

COMMIT;
