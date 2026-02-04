CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE audit.ddl_log (
  id BIGSERIAL PRIMARY KEY,
  ddl_tag TEXT NOT NULL,
  object_type TEXT,
  object_identity TEXT,
  command_text TEXT,
  executing_user TEXT DEFAULT CURRENT_USER,
  created_at TIMESTAMP DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION audit.log_ddl_event()
RETURNS event_trigger
SECURITY DEFINER
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF r.schema_name = 'audit' AND r.object_identity LIKE 'audit.ddl_log%' THEN
            CONTINUE;
        END IF;

        INSERT INTO audit.ddl_log
            (ddl_tag, object_type, object_identity, command_text)
        VALUES
            (
              r.command_tag,
              r.object_type,
              r.object_identity,
              current_query()
            );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION audit.log_drop_event()
RETURNS event_trigger
SECURITY DEFINER
AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF r.schema_name = 'audit' AND r.object_identity LIKE 'audit.ddl_log%' THEN
            CONTINUE;
        END IF;

        INSERT INTO audit.ddl_log
            (ddl_tag, object_type, object_identity, command_text)
        VALUES
            (
              'DROP',
              r.object_type,
              r.object_identity,
              current_query()
            );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS capture_ddl_trigger;
CREATE EVENT TRIGGER capture_ddl_trigger
    ON ddl_command_end
    EXECUTE FUNCTION audit.log_ddl_event();

DROP EVENT TRIGGER IF EXISTS log_drop;
CREATE EVENT TRIGGER log_drop
    ON sql_drop
    EXECUTE FUNCTION audit.log_drop_event();
