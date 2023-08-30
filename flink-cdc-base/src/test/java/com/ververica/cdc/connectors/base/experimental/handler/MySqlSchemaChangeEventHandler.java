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

package com.ververica.cdc.connectors.base.experimental.handler;

import com.ververica.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;
import io.debezium.schema.SchemaChangeEvent;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.BINLOG_FILENAME_OFFSET_KEY;
import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.BINLOG_POSITION_OFFSET_KEY;
import static com.ververica.cdc.connectors.base.experimental.offset.BinlogOffset.SERVER_ID_KEY;

/** MySqlSchemaChangeEventHandler to deal MySql's schema change event. */
public class MySqlSchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        Map<String, Object> source = new HashMap<>();
        Struct sourceInfo = event.getSource();
        String fileName = sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY);
        Long pos = sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY);
        Long serverId = sourceInfo.getInt64(SERVER_ID_KEY);
        source.put(SERVER_ID_KEY, serverId);
        source.put(BINLOG_FILENAME_OFFSET_KEY, fileName);
        source.put(BINLOG_POSITION_OFFSET_KEY, pos);
        return source;
    }
}
