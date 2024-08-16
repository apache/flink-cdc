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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
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

import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.v2.RecordSerializationSchema;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSinkContext;
import com.starrocks.connector.flink.tools.JsonWrapper;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Serializer for the input {@link Event}. It will serialize a row to a json string. */
public class EventRecordSerializationSchema implements RecordSerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    /**
     * The local time zone used when converting from <code>TIMESTAMP WITH LOCAL TIME ZONE</code>.
     */
    private final ZoneId zoneId;

    /** keep the relationship of TableId and table information. */
    private transient Map<TableId, TableInfo> tableInfoMap;

    private transient DefaultStarRocksRowData reusableRowData;
    private transient JsonWrapper jsonWrapper;

    public EventRecordSerializationSchema(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, StarRocksSinkContext sinkContext) {
        this.tableInfoMap = new HashMap<>();
        this.reusableRowData = new DefaultStarRocksRowData();
        this.jsonWrapper = new JsonWrapper();
    }

    @Override
    public StarRocksRowData serialize(Event record) {
        if (record instanceof SchemaChangeEvent) {
            applySchemaChangeEvent((SchemaChangeEvent) record);
            return null;
        } else if (record instanceof DataChangeEvent) {
            return applyDataChangeEvent((DataChangeEvent) record);
        } else {
            throw new UnsupportedOperationException("Don't support event " + record);
        }
    }

    private void applySchemaChangeEvent(SchemaChangeEvent event) {
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
                    StarRocksUtils.createFieldGetter(
                            newSchema.getColumns().get(i).getType(), i, zoneId);
        }
        tableInfoMap.put(tableId, tableInfo);
    }

    private StarRocksRowData applyDataChangeEvent(DataChangeEvent event) {
        TableInfo tableInfo = tableInfoMap.get(event.tableId());
        Preconditions.checkNotNull(tableInfo, event.tableId() + " is not existed");
        reusableRowData.setDatabase(event.tableId().getSchemaName());
        reusableRowData.setTable(event.tableId().getTableName());
        String value;
        switch (event.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                value = serializeRecord(tableInfo, event.after(), false);
                break;
            case DELETE:
                value = serializeRecord(tableInfo, event.before(), true);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Don't support operation type " + event.op());
        }
        reusableRowData.setRow(value);
        return reusableRowData;
    }

    private String serializeRecord(TableInfo tableInfo, RecordData record, boolean isDelete) {
        List<Column> columns = tableInfo.schema.getColumns();
        Preconditions.checkArgument(columns.size() == record.getArity());
        Map<String, Object> rowMap = new HashMap<>(record.getArity() + 1);
        for (int i = 0; i < record.getArity(); i++) {
            rowMap.put(columns.get(i).getName(), tableInfo.fieldGetters[i].getFieldOrNull(record));
        }
        rowMap.put("__op", isDelete ? 1 : 0);
        return jsonWrapper.toJSONString(rowMap);
    }

    @Override
    public void close() {}

    /** Table information. */
    private static class TableInfo {
        Schema schema;
        RecordData.FieldGetter[] fieldGetters;
    }
}
