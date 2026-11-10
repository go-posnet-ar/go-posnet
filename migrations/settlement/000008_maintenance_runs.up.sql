-- Tareas de mantenimiento que hay que ejecutar una vez por entorno. El servicio informa con
-- posnet_settlement_reversal_recovery_pending, y la alerta ReversasSinRecuperar, mientras falte la
-- de 'reversal-recovery': la registra settlement-recover-reversals cuando no quedan reversas
-- anteriores sin descontar (Escenario E del runbook batch-close-failure).
--
-- No se inserta ninguna fila: una base existente puede tener reversas que recuperar. Una base
-- nueva, creada con 01-init.sql, ya trae la fila.
--
-- ATENCIÓN: /readyz responde 503 sin esta tabla, y /metrics deja de informar los lotes que
-- requieren atención. Se puede aplicar con el servicio anterior en marcha.

BEGIN;

CREATE TABLE IF NOT EXISTS settlement.maintenance_runs (
    name         TEXT        PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_by TEXT        NOT NULL,
    details      TEXT
);

COMMIT;
