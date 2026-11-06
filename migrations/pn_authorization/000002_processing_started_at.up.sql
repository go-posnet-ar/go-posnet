-- Momento en que una transacción pasó a PROCESSING. El conciliador lo usa para distinguir
-- una llamada en curso al adquirente de una que quedó trabada.
--
-- ATENCIÓN: cada score de fraude escribe esta columna. Sin ella, ninguna venta llega al
-- adquirente y, al agotar los reintentos, sus eventos se pierden. Aplicar antes de desplegar.
--
-- Al desplegar, el conciliador revierte en el adquirente las INDETERMINATE que ya existan:
-- revisarlas antes si hay un adquirente real conectado.

BEGIN;

ALTER TABLE pn_authorization.transactions
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_auth_txn_unresolved
    ON pn_authorization.transactions (created_at)
    WHERE state IN ('PROCESSING', 'INDETERMINATE');

COMMIT;
