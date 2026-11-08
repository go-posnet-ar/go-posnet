-- Índice para el timeout de fraude: cada segundo, Authorization busca las transacciones que
-- esperan el score desde antes del corte. Sin él, cada búsqueda recorre la tabla entera.
--
-- CONCURRENTLY: se crea con el servicio en marcha, sin bloquear las escrituras. Por eso no va
-- dentro de una transacción. Si falla a mitad, queda marcado INVALID y IF NOT EXISTS lo
-- saltearía: hay que borrarlo (DROP INDEX CONCURRENTLY) y volver a aplicar este archivo.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_auth_txn_fraud_checking
    ON pn_authorization.transactions (created_at)
    WHERE state = 'FRAUD_CHECKING';
