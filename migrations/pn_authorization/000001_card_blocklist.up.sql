-- Blocklist de tarjetas y token de tarjeta en las transacciones.
--
-- Acompaña la funcionalidad de pick-up card: cuando el emisor responde
-- ISO 8583 04/41/43 el terminal retiene la tarjeta y esta queda bloqueada
-- para los intentos siguientes.
--
-- Idempotente a propósito: en entornos nuevos las tablas base ya vienen del
-- init script de Postgres (deployments/docker/docker-entrypoint-initdb.d/
-- 01-init.sql), así que este script debe poder correr sobre una base que ya
-- tiene los objetos y sobre una que no.

BEGIN;

-- ─── transactions.card_token ─────────────────────────────────────────────────
--
-- HMAC-SHA256 del PAN derivado en el borde (terminal/HSM), en hexadecimal
-- minúscula. NULL cuando el terminal todavía no lo emite.
--
-- Se persiste porque la Saga de autorización es asíncrona: cuando llega la
-- respuesta del adquirente el aggregate ya fue releído desde esta tabla, y sin
-- el token no habría con qué bloquear la tarjeta que el emisor mandó retener.
--
-- Nullable y sin default: las filas históricas quedan en NULL, que es
-- exactamente su significado — transacciones sin token, no bloqueables.
-- El PAN completo nunca llega al backend, así que no se puede backfillear.
ALTER TABLE pn_authorization.transactions
    ADD COLUMN IF NOT EXISTS card_token CHAR(64);

-- Índice parcial: solo las filas con token se consultan por token, y dejar
-- fuera los NULL evita indexar el grueso de la tabla mientras el borde no
-- emita el valor.
CREATE INDEX IF NOT EXISTS idx_auth_txn_card_token
    ON pn_authorization.transactions(card_token)
    WHERE card_token IS NOT NULL;

-- ─── blocked_cards ───────────────────────────────────────────────────────────
--
-- Se indexa por card_token y no por PAN ni por last4:
--   - El PAN completo nunca llega al backend Go (viaja cifrado en el EMV).
--   - last4 + red colisiona con ~1 de cada 10.000 tarjetas legítimas, o sea
--     bloquearía titulares inocentes.
--
-- No se guarda last4 ni red: no son clave utilizable y solo agregarían datos
-- de tarjeta en reposo sin ningún uso.
CREATE TABLE IF NOT EXISTS pn_authorization.blocked_cards (
    -- HMAC del PAN. PK: una tarjeta se bloquea una sola vez y el INSERT del
    -- repositorio usa ON CONFLICT DO NOTHING para conservar el primer bloqueo.
    card_token            CHAR(64)    PRIMARY KEY,

    -- Código que originó el bloqueo: "04", "41" o "43".
    reason                VARCHAR(40) NOT NULL,

    -- Transacción que detectó la tarjeta primero. Sin FK a transactions: la
    -- blocklist debe sobrevivir a cualquier purga de transacciones históricas.
    source_transaction_id UUID,

    blocked_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
