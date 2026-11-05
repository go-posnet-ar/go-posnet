-- Integridad de los lotes de liquidación.
--
-- Debe aplicarse ANTES de desplegar la versión del servicio que lo requiere: esa
-- versión lee y escribe settlement_batches.version. Idempotente a propósito: en
-- entornos nuevos estos objetos ya vienen del init script de Postgres
-- (deployments/docker/docker-entrypoint-initdb.d/01-init.sql).

BEGIN;

-- ─── settlement_batches ──────────────────────────────────────────────────────
--
-- version: bloqueo optimista. Evita que un guardado con datos viejos pise a otro,
-- por ejemplo una venta concurrente que devolvía a OPEN un lote recién cerrado.
ALTER TABLE settlement.settlement_batches
    ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 0;

-- Antes había un lote por terminal y fecha en cualquier estado, así que una venta
-- posterior al cierre no tenía lote donde entrar. Ahora la unicidad aplica solo al
-- lote abierto, y por moneda, para no sumar montos de monedas distintas.
ALTER TABLE settlement.settlement_batches
    DROP CONSTRAINT IF EXISTS settlement_batches_terminal_id_batch_date_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_settlement_batches_open
    ON settlement.settlement_batches (terminal_id, batch_date, currency)
    WHERE state = 'OPEN';

-- ─── batch_transactions ──────────────────────────────────────────────────────
--
-- La reversa usa el transaction_id de la compra. Con la unicidad solo por
-- transaction_id, su fila se descartaba sin error y nunca se descontaba.
ALTER TABLE settlement.batch_transactions
    DROP CONSTRAINT IF EXISTS batch_transactions_transaction_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_transactions_tx_type
    ON settlement.batch_transactions (transaction_id, tx_type);

COMMIT;
