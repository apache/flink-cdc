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

package com.ververica.cdc.connectors.doris.sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.common.utils.SchemaUtils;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.doris.flink.sink.util.DeleteOperation.addDeleteSign;

/** A serializer for Event to DorisRecord. */
public class DorisEventSerializer implements DorisRecordSerializer<Event> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private Map<TableId, Schema> schemaMaps = new HashMap<>();

    /** Format DATE type data. */
    public static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    /** Format timestamp-related type data. */
    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** ZoneId from pipeline config to support timestamp with local time zone. */
    public final ZoneId pipelineZoneId;

    public DorisEventSerializer(ZoneId zoneId) {
        pipelineZoneId = zoneId;
    }

    @Override
    public DorisRecord serialize(Event event) throws IOException {
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

    private DorisRecord applyDataChangeEvent(DataChangeEvent event) throws JsonProcessingException {
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
                addDeleteSign(valueMap, false);
                break;
            case DELETE:
                valueMap = serializerRecord(event.before(), schema);
                addDeleteSign(valueMap, true);
                break;
            default:
                throw new UnsupportedOperationException("Unsupport Operation " + op);
        }

        return DorisRecord.of(
                tableId.getSchemaName(),
                tableId.getTableName(),
                objectMapper.writeValueAsString(valueMap).getBytes(StandardCharsets.UTF_8));
    }

    /** serializer RecordData to Doris Value. */
    public Map<String, Object> serializerRecord(RecordData recordData, Schema schema) {
        List<Column> columns = schema.getColumns();
        Map<String, Object> record = new HashMap<>();
        Preconditions.checkState(
                columns.size() == recordData.getArity(),
                "Column size does not match the data size");

        for (int i = 0; i < recordData.getArity(); i++) {
            DorisRowConverter.SerializationConverter converter =
                    DorisRowConverter.createNullableExternalConverter(
                            columns.get(i).getType(), pipelineZoneId);
            Object field = converter.serialize(i, recordData);
            record.put(columns.get(i).getName(), field);
        }
        return record;
    }
}
