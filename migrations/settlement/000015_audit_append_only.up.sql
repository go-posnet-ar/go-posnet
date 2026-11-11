-- La auditoría de lotes solo admite altas. Un trigger rechaza UPDATE, DELETE y TRUNCATE sobre
-- batch_audit_log, para cualquier usuario: el servicio solo inserta, y ninguna migración anterior
-- modifica filas.
--
-- Protege contra un cambio accidental o desde la aplicación, no contra el dueño de la tabla o un
-- superusuario, que pueden desactivar el trigger (ALTER TABLE ... DISABLE TRIGGER). Para eso, el
-- servicio debería usar un rol que no sea el dueño: ver "Auditoría de operadores" en
-- docs/technical/SETTLEMENT.md.
--
-- Se puede aplicar con el servicio anterior en marcha: tampoco modifica filas.

BEGIN;

CREATE OR REPLACE FUNCTION settlement.reject_audit_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'settlement.batch_audit_log is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

DROP TRIGGER IF EXISTS batch_audit_log_no_update ON settlement.batch_audit_log;
CREATE TRIGGER batch_audit_log_no_update
    BEFORE UPDATE OR DELETE ON settlement.batch_audit_log
    FOR EACH ROW EXECUTE FUNCTION settlement.reject_audit_change();

DROP TRIGGER IF EXISTS batch_audit_log_no_truncate ON settlement.batch_audit_log;
CREATE TRIGGER batch_audit_log_no_truncate
    BEFORE TRUNCATE ON settlement.batch_audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION settlement.reject_audit_change();

COMMIT;
