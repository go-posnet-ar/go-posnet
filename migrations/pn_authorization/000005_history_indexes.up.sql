-- Índices para las consultas de historial de Fraud Detection sobre esta tabla: ventas de un
-- terminal en un período (velocidad, rechazos recientes, montos repetidos) y promedio aprobado de
-- un comercio en 30 días. Sin ellos, cada evaluación de fraude recorre la tabla entera.
--
-- CONCURRENTLY: se crean con el servicio en marcha, sin bloquear las escrituras. Por eso no van
-- dentro de una transacción. Si uno falla a mitad, queda marcado INVALID y IF NOT EXISTS lo
-- saltearía: hay que borrarlo (DROP INDEX CONCURRENTLY) y volver a aplicar este archivo.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_auth_txn_terminal_created
    ON pn_authorization.transactions (terminal_id, created_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_auth_txn_merchant_approved
    ON pn_authorization.transactions (merchant_id, created_at)
    WHERE state = 'APPROVED';
