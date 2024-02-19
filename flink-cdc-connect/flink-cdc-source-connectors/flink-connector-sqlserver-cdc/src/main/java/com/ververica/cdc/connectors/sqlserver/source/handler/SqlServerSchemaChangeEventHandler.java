/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.source.handler;

import com.ververica.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;
import io.debezium.schema.SchemaChangeEvent;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

import static io.debezium.connector.sqlserver.SourceInfo.CHANGE_LSN_KEY;
import static io.debezium.connector.sqlserver.SourceInfo.COMMIT_LSN_KEY;
import static io.debezium.connector.sqlserver.SourceInfo.EVENT_SERIAL_NO_KEY;

/**
 * This SqlServerSchemaChangeEventHandler helps to parse the source struct in SchemaChangeEvent and
 * generate source info.
 */
public class SqlServerSchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        Map<String, Object> source = new HashMap<>();
        Struct sourceInfo = event.getSource();
        String changeLsn = sourceInfo.getString(CHANGE_LSN_KEY);
        String commitLsn = sourceInfo.getString(COMMIT_LSN_KEY);
        Long eventSerialNo = sourceInfo.getInt64(EVENT_SERIAL_NO_KEY);
        source.put(CHANGE_LSN_KEY, changeLsn);
        source.put(COMMIT_LSN_KEY, commitLsn);
        source.put(EVENT_SERIAL_NO_KEY, eventSerialNo);
        return source;
    }
}
