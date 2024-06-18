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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Serialization schema between {@link Event} and {@link JdbcRowData}. */
public class EventRecordSerializationSchema implements RecordSerializationSchema<Event> {
    /** Keeping the relationship of TableId and table information. */
    private final Map<TableId, TableInfo> tableInfoMap;

    private final Map<TableId, Boolean> tableHasPrimaryKeyMap;
    private final JsonWrapper jsonWrapper;

    public EventRecordSerializationSchema() {
        tableInfoMap = new HashMap<>();
        tableHasPrimaryKeyMap = new HashMap<>();
        jsonWrapper = new JsonWrapper();
    }

    @Override
    public RichJdbcRowData serialize(Event record) throws IOException {
        if (record instanceof SchemaChangeEvent) {
            return applySchemaChangeEvent((SchemaChangeEvent) record);
        } else if (record instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) record);
        } else {
            throw new UnsupportedOperationException("Unsupported event record type: " + record);
        }
    }

    private RichJdbcRowData applySchemaChangeEvent(SchemaChangeEvent event) {
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
        TableInfo tableInfo = new TableInfo();
        tableInfo.schema = newSchema;
        tableInfo.fieldGetters = new RecordData.FieldGetter[newSchema.getColumnCount()];
        for (int i = 0; i < newSchema.getColumnCount(); i++) {
            tableInfo.fieldGetters[i] =
                    RecordData.createFieldGetter(newSchema.getColumns().get(i).getType(), i);
        }

        tableInfoMap.put(tableId, tableInfo);
        tableHasPrimaryKeyMap.put(tableId, !newSchema.primaryKeys().isEmpty());

        RichJdbcRowData reusableRowData = new RichJdbcRowData();
        reusableRowData.setRowKind(RowKind.SCHEMA_CHANGE);
        reusableRowData.setTableId(event.tableId());
        return reusableRowData;
    }

    private RichJdbcRowData applyDataChangeEvent(DataChangeEvent event)
            throws JsonProcessingException {
        TableInfo tableInfo = tableInfoMap.get(event.tableId());
        Preconditions.checkNotNull(tableInfo, event.tableId() + " is not existed");

        RichJdbcRowData reusableRowData = new RichJdbcRowData();
        reusableRowData.setTableId(event.tableId());
        reusableRowData.setSchema(tableInfo.schema);

        byte[] value = null;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                reusableRowData.setRowKind(RowKind.INSERT);
                value = serializeRecord(event.tableId(), tableInfo, event.after());
                break;
            case DELETE:
                reusableRowData.setRowKind(RowKind.DELETE);
                value = serializeRecord(event.tableId(), tableInfo, event.before());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support operation type " + event.op());
        }

        reusableRowData.setRows(value);
        reusableRowData.setHasPrimaryKey(tableHasPrimaryKeyMap.get(event.tableId()));
        return reusableRowData;
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

        return jsonWrapper.toJSONString(rowMap).getBytes(StandardCharsets.UTF_8);
    }

    /** Table information. */
    private static class TableInfo {
        Schema schema;
        RecordData.FieldGetter[] fieldGetters;
    }
}
