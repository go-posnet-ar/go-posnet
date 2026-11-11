-- Revisiones de settlement.completed. El resumen de liquidación de un comercio, un día y una moneda
-- sale cuando se liquidan todos sus lotes, y otra vez por cada lote que se liquida después. Antes,
-- cada uno repetía el acumulado sin decir que era otro, y la liquidación tardía de una moneda
-- repetía también el de las demás. Ahora cada resumen lleva su revisión y la diferencia con el
-- anterior, y una moneda que no cambió no se repite.
--
-- Completa la revisión 1 de los días ya liquidados del todo, con los totales actuales: el resumen
-- siguiente de esos días sale como revisión 2, con su diferencia. net_amount y event_id quedan
-- vacíos: el MDR de entonces no se guardaba. Un día que ya había emitido más de un resumen queda
-- con una sola fila, con el acumulado: el siguiente igual sale con la diferencia correcta.
--
-- ATENCIÓN: /readyz responde 503 sin esta tabla. Se puede aplicar con el servicio anterior en
-- marcha: no la usa.

BEGIN;

CREATE TABLE IF NOT EXISTS settlement.settlement_completions (
    merchant_id       UUID        NOT NULL,
    settlement_date   DATE        NOT NULL,
    currency          CHAR(3)     NOT NULL,
    revision          INT         NOT NULL,
    total_batches     INT         NOT NULL,
    total_amount      BIGINT      NOT NULL,
    adjustment_amount BIGINT      NOT NULL,
    net_amount        BIGINT,
    event_id          TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, settlement_date, currency, revision)
);

INSERT INTO settlement.settlement_completions
    (merchant_id, settlement_date, currency, revision, total_batches, total_amount, adjustment_amount)
SELECT b.merchant_id, b.batch_date, b.currency, 1, COUNT(*),
       SUM(COALESCE(b.total_amount, 0)), SUM(COALESCE(b.adjustment_amount, 0))
FROM settlement.settlement_batches b
WHERE EXISTS (SELECT 1 FROM settlement.batch_transactions t WHERE t.batch_id = b.id)
GROUP BY b.merchant_id, b.batch_date, b.currency
-- El mismo criterio que la liquidación: todos los lotes con transacciones del comercio y el día,
-- de cualquier moneda, están SETTLED.
HAVING NOT EXISTS (
    SELECT 1 FROM settlement.settlement_batches o
    WHERE o.merchant_id = b.merchant_id AND o.batch_date = b.batch_date AND o.state <> 'SETTLED'
      AND EXISTS (SELECT 1 FROM settlement.batch_transactions t WHERE t.batch_id = o.id)
)
ON CONFLICT DO NOTHING;

COMMIT;
