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

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.jdbc.sink.utils.JsonWrapper;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Serialization schema between {@link Event} and {@link JdbcRowData}. */
public class EventRecordSerializationSchema implements RecordSerializationSchema<Event> {
    /** Keeping the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    private final JsonWrapper jsonWrapper = new JsonWrapper();

    public EventRecordSerializationSchema() {
        tableInfoMap = new HashMap<>();
    }

    @Override
    public JdbcRowData[] serialize(Event record) throws IOException {
        if (record instanceof SchemaChangeEvent) {
            return new JdbcRowData[] {applySchemaChangeEvent((SchemaChangeEvent) record)};
        } else if (record instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) record);
        } else {
            throw new UnsupportedOperationException("Unsupported event record type: " + record);
        }
    }

    private JdbcRowData applySchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("schema of " + tableId + " is not existed.");
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.schema, event);
        }
        TableInfo tableInfo = new TableInfo(newSchema);
        tableInfoMap.put(tableId, tableInfo);

        return new RichJdbcRowData.Builder()
                .setRowKind(RowKind.SCHEMA_CHANGE)
                .setTableId(event.tableId())
                .build();
    }

    private JdbcRowData[] applyDataChangeEvent(DataChangeEvent event)
            throws JsonProcessingException {
        TableInfo tableInfo = tableInfoMap.get(event.tableId());
        Preconditions.checkNotNull(tableInfo, event.tableId() + " does not exist");

        RichJdbcRowData.Builder builder =
                new RichJdbcRowData.Builder()
                        .setTableId(event.tableId())
                        .setSchema(tableInfo.schema);

        List<JdbcRowData> rows = new ArrayList<>(2);

        if (event.before() != null) {
            builder.setRowKind(RowKind.DELETE);
            builder.setRows(serializeRecord(event.tableId(), tableInfo, event.before()));
            rows.add(builder.build());
        }

        if (event.after() != null) {
            builder.setRowKind(RowKind.INSERT);
            builder.setRows(serializeRecord(event.tableId(), tableInfo, event.after()));
            rows.add(builder.build());
        }

        return rows.toArray(new JdbcRowData[0]);
    }

    private byte[] serializeRecord(TableId tableId, TableInfo tableInfo, RecordData record)
            throws JsonProcessingException {
        Preconditions.checkNotNull(record, tableId + " record is null");

        List<Column> columns = tableInfo.schema.getColumns();
        Preconditions.checkArgument(columns.size() == record.getArity());

        Map<String, Object> rowMap = new HashMap<>(record.getArity() + 1);
        for (int i = 0; i < record.getArity(); i++) {
            rowMap.put(columns.get(i).getName(), tableInfo.fieldGetters[i].getFieldOrNull(record));
        }

        return jsonWrapper.toJSONBytes(rowMap);
    }

    /** Table information. */
    private static class TableInfo {
        Schema schema;
        RecordData.FieldGetter[] fieldGetters;

        public TableInfo(Schema schema) {
            this.schema = schema;
            this.fieldGetters =
                    SchemaUtils.createFieldGetters(schema).toArray(new RecordData.FieldGetter[0]);
        }
    }
}
