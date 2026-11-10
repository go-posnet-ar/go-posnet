-- Totales de un cierre del terminal que no encontró ningún lote del día (no_batch): el terminal
-- informó ventas que el backend todavía no tiene, porque se están reintentando o se perdieron.
-- Antes quedaban solo en el log, y las ventas que llegaban después formaban un lote que el cierre
-- automático cerraba sin conciliar. Ahora ese cierre los usa para conciliar, como si el terminal
-- hubiera pedido el cierre en ese momento, y borra la fila. Un cierre posterior del terminal para
-- el mismo día también la borra: sus totales son los más nuevos.
--
-- Una fila que nunca se consume es un día sin ninguna venta en el backend: la alerta CierreSinLote
-- ya lo informó.
--
-- ATENCIÓN: /readyz responde 503 sin esta tabla. Se puede aplicar con el servicio anterior en
-- marcha: no la usa.

BEGIN;

CREATE TABLE IF NOT EXISTS settlement.pending_terminal_closes (
    terminal_id     UUID        NOT NULL,
    batch_date      DATE        NOT NULL,
    currency        CHAR(3)     NOT NULL,
    terminal_count  INT         NOT NULL,
    terminal_amount BIGINT      NOT NULL,
    event_id        TEXT        NOT NULL,
    requested_at    TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (terminal_id, batch_date, currency)
);

COMMIT;
