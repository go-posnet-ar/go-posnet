-- Índices de las consultas frecuentes de lotes.
--
-- Sin idx_batch_transactions_batch, cargar un lote recorre toda la tabla de
-- transacciones, y eso ocurre en cada venta.
--
-- CONCURRENTLY no bloquea las escrituras mientras se crea el índice, pero no puede
-- correr dentro de una transacción: ejecutar con psql -f, sin -1 ni BEGIN. Si se
-- interrumpe, puede dejar un índice inválido: borrarlo y volver a ejecutar.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_batch_transactions_batch
    ON settlement.batch_transactions (batch_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_settlement_batches_merchant_date
    ON settlement.settlement_batches (merchant_id, batch_date DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_settlement_batches_state
    ON settlement.settlement_batches (state)
    WHERE state IN ('OPEN', 'PENDING_CLOSE', 'CLOSED', 'SUBMITTED', 'DISPUTED');
