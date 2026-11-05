-- Quita los índices de lotes. No afecta los datos, pero vuelve lentas las consultas.

DROP INDEX CONCURRENTLY IF EXISTS settlement.idx_settlement_batches_state;
DROP INDEX CONCURRENTLY IF EXISTS settlement.idx_settlement_batches_merchant_date;
DROP INDEX CONCURRENTLY IF EXISTS settlement.idx_batch_transactions_batch;
