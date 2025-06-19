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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.fluss.flink.row.OperationType.DELETE;
import static com.alibaba.fluss.flink.row.OperationType.UPSERT;

public class FlussEventSerializationSchema implements FlussSerializationSchema<Event> {
    private static final long serialVersionUID = 1L;

    private transient Map<TableId, TableSchemaInfo> tableInfoMap;
    private final ZoneId zoneId;

    public FlussEventSerializationSchema(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public void open(InitializationContext initializationContext) throws Exception {
        this.tableInfoMap = new HashMap<>();
    }

    @Override
    public RowWithOp serialize(Event record) throws Exception {
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
        org.apache.flink.cdc.common.schema.Schema newSchema;
        if (event instanceof CreateTableEvent) {
            newSchema = ((CreateTableEvent) event).getSchema();
        } else {
            TableSchemaInfo tableInfo = tableInfoMap.get(tableId);
            if (tableInfo == null) {
                throw new RuntimeException("schema of " + tableId + " is not existed.");
            }
            newSchema = SchemaUtils.applySchemaChangeEvent(tableInfo.schema, event);
        }
        tableInfoMap.put(tableId, new TableSchemaInfo(newSchema, zoneId));
    }

    private RowWithOp applyDataChangeEvent(DataChangeEvent record) {
        OperationType op = record.op();
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                return new RowWithOp(
                        serializeRecord(tableInfoMap.get(record.tableId()), record.after()),
                        UPSERT);
            case DELETE:
                return new RowWithOp(
                        serializeRecord(tableInfoMap.get(record.tableId()), record.before()),
                        DELETE);
            default:
                throw new IllegalArgumentException("Unsupported row kind: " + op);
        }
    }

    private InternalRow serializeRecord(TableSchemaInfo tableInfo, RecordData record) {
        List<Column> columns = tableInfo.schema.getColumns();
        Preconditions.checkArgument(columns.size() == record.getArity());
        GenericRow row = new GenericRow(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            row.setField(i, tableInfo.fieldGetters[i].getFieldOrNull(record));
        }
        return row;
    }
}
