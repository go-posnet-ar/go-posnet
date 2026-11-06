-- Zona horaria del día de negocio por comercio. Sin fila, rige SETTLEMENT_BUSINESS_TIMEZONE.
--
-- ATENCIÓN: cada venta, reversa y cierre consultan esta tabla. Sin ella, fallan todos y, al
-- agotar los reintentos, sus eventos se pierden. Aplicar antes de desplegar.

BEGIN;

CREATE TABLE IF NOT EXISTS settlement.merchant_timezones (
    merchant_id UUID        PRIMARY KEY,
    timezone    TEXT        NOT NULL,
    updated_by  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
