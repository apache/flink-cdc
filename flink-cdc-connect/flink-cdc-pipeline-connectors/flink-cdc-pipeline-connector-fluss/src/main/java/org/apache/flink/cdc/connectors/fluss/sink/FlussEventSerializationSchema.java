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

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussEvent;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussRecordSerializer;
import org.apache.flink.cdc.connectors.fluss.sink.v2.RowWithOp;

import com.alibaba.fluss.metadata.TablePath;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.fluss.sink.v2.OperationType.APPEND;
import static org.apache.flink.cdc.connectors.fluss.sink.v2.OperationType.DELETE;
import static org.apache.flink.cdc.connectors.fluss.sink.v2.OperationType.UPSERT;

/** Serialization schema that converts a CDC data record to a Fluss event. */
public class FlussEventSerializationSchema implements FlussRecordSerializer<Event> {
    private static final long serialVersionUID = 1L;

    private transient Map<TableId, Schema> tableInfoMap;

    @Override
    public void open() {
        this.tableInfoMap = new HashMap<>();
    }

    @Override
    public FlussEvent serialize(Event record) throws IOException {
        if (record instanceof SchemaChangeEvent) {
            applySchemaChangeEvent((SchemaChangeEvent) record);
            return new FlussEvent(getTablePath(((SchemaChangeEvent) record).tableId()), null, true);
        } else if (record instanceof DataChangeEvent) {
            RowWithOp rowWithOp = applyDataChangeEvent((DataChangeEvent) record);
            return new FlussEvent(
                    getTablePath(((DataChangeEvent) record).tableId()),
                    Collections.singletonList(rowWithOp),
                    false);

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
            // TODO: Logics for altering tables are not supported yet.
            // This is anticipated to be supported in Fluss version 0.8.0.
            throw new RuntimeException(
                    "Schema change type not supported. Only CreateTableEvent is allowed at the moment.");
        }
        tableInfoMap.put(tableId, newSchema);
    }

    private RowWithOp applyDataChangeEvent(DataChangeEvent record) {
        OperationType op = record.op();
        Schema schema = tableInfoMap.get(record.tableId());
        boolean hasPrimaryKey = !schema.primaryKeys().isEmpty();
        Preconditions.checkNotNull(schema, "Table schema not found for table " + record.tableId());
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                return new RowWithOp(
                        CdcAsFlussRow.replace(record.after()), hasPrimaryKey ? UPSERT : APPEND);
            case DELETE:
                return new RowWithOp(CdcAsFlussRow.replace(record.before()), DELETE);
            default:
                throw new IllegalArgumentException("Unsupported row kind: " + op);
        }
    }

    private TablePath getTablePath(TableId tableId) {
        return TablePath.of(tableId.getSchemaName(), tableId.getTableName());
    }
}
