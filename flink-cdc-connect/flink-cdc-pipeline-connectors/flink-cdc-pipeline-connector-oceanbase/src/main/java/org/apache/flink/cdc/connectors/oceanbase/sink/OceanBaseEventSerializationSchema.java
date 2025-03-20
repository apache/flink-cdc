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

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** A serializer for Event to Record. */
public class OceanBaseEventSerializationSchema implements RecordSerializationSchema<Event> {

    private final Cache<Schema, List<OceanBaseRowConvert.SerializationConverter>> cache =
            CacheBuilder.newBuilder().build();
    private final Map<TableId, Schema> schemaMaps = new HashMap<>();

    /** ZoneId from pipeline config to support timestamp with local time zone. */
    public final ZoneId pipelineZoneId;

    public OceanBaseEventSerializationSchema(ZoneId zoneId) {
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
        Object[] values;
        OperationType op = event.op();
        boolean isDelete = false;
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                values = serializerRecord(event.after(), schema);
                break;
            case DELETE:
                values = serializerRecord(event.before(), schema);
                isDelete = true;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported Operation " + op);
        }
        return buildDataChangeRecord(tableId, schema, values, isDelete);
    }

    private DataChangeRecord buildDataChangeRecord(
            TableId tableId, Schema schema, Object[] values, boolean isDelete) {
        Preconditions.checkState(
                Objects.nonNull(tableId.getSchemaName()), "Schema name cannot be null or empty.");
        com.oceanbase.connector.flink.table.TableId oceanBaseTableId =
                new com.oceanbase.connector.flink.table.TableId(
                        tableId.getSchemaName(), tableId.getTableName());
        TableInfo tableInfo =
                new TableInfo(
                        oceanBaseTableId,
                        schema.primaryKeys(),
                        schema.getColumnNames(),
                        Lists.newArrayList(),
                        null);

        return new DataChangeRecord(
                tableInfo,
                isDelete ? DataChangeRecord.Type.DELETE : DataChangeRecord.Type.UPSERT,
                values);
    }

    /** serializer RecordData to oceanbase data change record. */
    public Object[] serializerRecord(RecordData recordData, Schema schema) {
        List<Column> columns = schema.getColumns();
        Preconditions.checkState(
                columns.size() == recordData.getArity(),
                "Column size does not match the data size");
        Object[] values = new Object[columns.size()];

        List<OceanBaseRowConvert.SerializationConverter> converters = null;
        try {
            converters =
                    cache.get(
                            schema,
                            () ->
                                    columns.stream()
                                            .map(
                                                    column ->
                                                            OceanBaseRowConvert
                                                                    .createNullableExternalConverter(
                                                                            column.getType(),
                                                                            pipelineZoneId))
                                            .collect(Collectors.toList()));
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to obtain SerializationConverter cache", e);
        }

        for (int i = 0; i < recordData.getArity(); i++) {
            Object field = converters.get(i).serialize(i, recordData);
            values[i] = field;
        }

        return values;
    }
}
