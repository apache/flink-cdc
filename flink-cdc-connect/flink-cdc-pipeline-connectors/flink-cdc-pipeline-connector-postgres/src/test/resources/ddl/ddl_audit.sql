-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create event listener table and associate it with event triggers to listen for ddl_command_end and sql_drop events.
DROP SCHEMA IF EXISTS audit CASCADE;
CREATE SCHEMA audit;

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
        IF r.object_type = 'table column' THEN
            CONTINUE;
        END IF;
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
