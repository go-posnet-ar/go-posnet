-- Transacciones rechazadas por datos inválidos. No entran en transactions porque no cumplen sus
-- restricciones, y Fraud Detection lee esa tabla. Permiten distinguir un rechazo de un ID que
-- nunca llegó.
--
-- Solo guarda valores validados: nunca el payload original, porque card_last4 llega sin validar
-- y podría traer un PAN completo.
--
-- ATENCIÓN: sin esta tabla, las ventas válidas siguen funcionando, pero /readyz responde 503 y
-- Docker marca el contenedor unhealthy. Aplicar antes de desplegar.

CREATE TABLE IF NOT EXISTS pn_authorization.invalid_transactions (
    transaction_id    UUID        PRIMARY KEY,
    terminal_id       UUID,
    merchant_id       UUID,
    amount_cents      BIGINT      NOT NULL DEFAULT 0,
    currency          CHAR(3),
    rejection_code    VARCHAR(20) NOT NULL,
    validation_error  TEXT        NOT NULL,
    rejected_at       TIMESTAMPTZ NOT NULL
);
