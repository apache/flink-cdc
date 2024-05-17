/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A serializer for Event to Record. */
public class OceanBaseEventSerializer implements RecordSerializationSchema<Event> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private Map<TableId, Schema> schemaMaps = new HashMap<>();

    /** Format DATE type data. */
    public static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** ZoneId from pipeline config to support timestamp with local time zone. */
    public final ZoneId pipelineZoneId;

    public OceanBaseEventSerializer(ZoneId zoneId) {
        pipelineZoneId = zoneId;
    }

    @Override
    public Record serialize(Event event) {
        if (event instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) event);
        } else if (event instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            if (event instanceof CreateTableEvent) {
                schemaMaps.put(tableId, ((CreateTableEvent) event).getSchema());
            } else {
                if (!schemaMaps.containsKey(tableId)) {
                    throw new RuntimeException("schema of " + tableId + " is not existed.");
                }
                schemaMaps.put(
                        tableId,
                        SchemaUtils.applySchemaChangeEvent(
                                schemaMaps.get(tableId), schemaChangeEvent));
            }
        }
        return null;
    }

    private Record applyDataChangeEvent(DataChangeEvent event) {
        TableId tableId = event.tableId();
        Schema schema = schemaMaps.get(tableId);
        Preconditions.checkNotNull(schema, event.tableId() + " is not existed");
        Map<String, Object> valueMap;
        OperationType op = event.op();
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                valueMap = serializerRecord(event.after(), schema);
                //                addDeleteSign(valueMap, false);
                break;
            case DELETE:
                valueMap = serializerRecord(event.before(), schema);
                //                addDeleteSign(valueMap, true);
                break;
            default:
                throw new UnsupportedOperationException("Unsupport Operation " + op);
        }

        return null;
    }

    /** serializer RecordData to Doris Value. */
    public Map<String, Object> serializerRecord(RecordData recordData, Schema schema) {
        List<Column> columns = schema.getColumns();
        Map<String, Object> record = new HashMap<>();
        Preconditions.checkState(
                columns.size() == recordData.getArity(),
                "Column size does not match the data size");

        return record;
    }
}
