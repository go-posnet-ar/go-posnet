-- Revierte la integridad de los lotes de liquidación.
--
-- ATENCIÓN: falla si ya hay más de un lote por terminal y fecha, o si hay filas
-- REVERSAL, porque las restricciones anteriores no las admiten. Resolver esos datos
-- es una decisión financiera: este script no los borra. Si falla, no cambia nada.

BEGIN;

DROP INDEX IF EXISTS settlement.uq_batch_transactions_tx_type;

ALTER TABLE settlement.batch_transactions
    ADD CONSTRAINT batch_transactions_transaction_id_key UNIQUE (transaction_id);

DROP INDEX IF EXISTS settlement.uq_settlement_batches_open;

ALTER TABLE settlement.settlement_batches
    ADD CONSTRAINT settlement_batches_terminal_id_batch_date_key UNIQUE (terminal_id, batch_date);

ALTER TABLE settlement.settlement_batches
    DROP COLUMN IF EXISTS version;

COMMIT;
