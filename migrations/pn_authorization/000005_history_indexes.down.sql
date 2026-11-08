-- Sin estos índices, las consultas de historial de Fraud Detection vuelven a recorrer la tabla.

DROP INDEX CONCURRENTLY IF EXISTS pn_authorization.idx_auth_txn_terminal_created;
DROP INDEX CONCURRENTLY IF EXISTS pn_authorization.idx_auth_txn_merchant_approved;
