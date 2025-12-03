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
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussEvent;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussEventSerializer;
import org.apache.flink.cdc.connectors.fluss.sink.v2.FlussRowWithOp;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.fluss.sink.v2.FlussOperationType.APPEND;
import static org.apache.flink.cdc.connectors.fluss.sink.v2.FlussOperationType.DELETE;
import static org.apache.flink.cdc.connectors.fluss.sink.v2.FlussOperationType.UPSERT;
import static org.apache.flink.cdc.connectors.fluss.utils.FlussConversions.sameCdcColumnsIgnoreCommentAndDefaultValue;
import static org.apache.flink.cdc.connectors.fluss.utils.FlussConversions.toFlussSchema;

/** Serialization schema that converts a CDC data record to a Fluss event. */
public class FlussEventSerializationSchema implements FlussEventSerializer<Event> {
    private static final long serialVersionUID = 1L;

    private transient Map<TableId, TableSchemaInfo> tableInfoMap;
    private transient Connection connection;

    @Override
    public void open(Connection connection) {
        this.tableInfoMap = new HashMap<>();
        this.connection = connection;
    }

    @Override
    public FlussEvent serialize(Event event) throws IOException {
        if (event instanceof SchemaChangeEvent) {
            applySchemaChangeEvent((SchemaChangeEvent) event);
            return new FlussEvent(getTablePath(((SchemaChangeEvent) event).tableId()), null, true);
        } else if (event instanceof DataChangeEvent) {
            FlussRowWithOp rowWithOp = applyDataChangeEvent((DataChangeEvent) event);
            return new FlussEvent(
                    getTablePath(((DataChangeEvent) event).tableId()),
                    Collections.singletonList(rowWithOp),
                    false);

        } else {
            throw new UnsupportedOperationException("Don't support event " + event);
        }
    }

    private void applySchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        if (event instanceof CreateTableEvent) {
            org.apache.flink.cdc.common.schema.Schema newSchema =
                    ((CreateTableEvent) event).getSchema();
            // if the table is not exist or the schema is changed, update the table info.
            if (!tableInfoMap.containsKey(tableId)
                    || !sameCdcColumnsIgnoreCommentAndDefaultValue(
                            tableInfoMap.get(tableId).upstreamCdcSchema, newSchema)) {
                Table table = connection.getTable(getTablePath(tableId));
                TableSchemaInfo newSchemaInfo =
                        new TableSchemaInfo(newSchema, table.getTableInfo().getSchema());
                tableInfoMap.put(tableId, newSchemaInfo);
            }
        } else {
            // TODO: Logics for altering tables are not supported yet.
            // This is anticipated to be supported in Fluss version 0.8.0.
            throw new RuntimeException(
                    "Schema change type not supported. Only CreateTableEvent is allowed at the moment.");
        }
    }

    private FlussRowWithOp applyDataChangeEvent(DataChangeEvent record) {
        OperationType op = record.op();
        TableSchemaInfo tableSchemaInfo = tableInfoMap.get(record.tableId());
        Preconditions.checkNotNull(
                tableSchemaInfo, "Table schema not found for table " + record.tableId());
        int flussFieldCount =
                tableSchemaInfo.downStreamFlusstreamSchema.getRowType().getFieldCount();
        boolean hasPrimaryKey = !tableSchemaInfo.upstreamCdcSchema.primaryKeys().isEmpty();
        switch (op) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                return new FlussRowWithOp(
                        CdcAsFlussRow.replace(
                                record.after(), flussFieldCount, tableSchemaInfo.indexMapping),
                        hasPrimaryKey ? UPSERT : APPEND);
            case DELETE:
                return new FlussRowWithOp(
                        CdcAsFlussRow.replace(
                                record.before(), flussFieldCount, tableSchemaInfo.indexMapping),
                        DELETE);
            default:
                throw new IllegalArgumentException("Unsupported row kind: " + op);
        }
    }

    private TablePath getTablePath(TableId tableId) {
        return TablePath.of(tableId.getSchemaName(), tableId.getTableName());
    }

    private static class TableSchemaInfo {
        org.apache.flink.cdc.common.schema.Schema upstreamCdcSchema;
        com.alibaba.fluss.metadata.Schema downStreamFlusstreamSchema;
        Map<Integer, Integer> indexMapping;

        private TableSchemaInfo(
                org.apache.flink.cdc.common.schema.Schema upstreamCdcSchema,
                com.alibaba.fluss.metadata.Schema downStreamFlusstreamSchema) {
            this.upstreamCdcSchema = upstreamCdcSchema;
            this.downStreamFlusstreamSchema = downStreamFlusstreamSchema;
            this.indexMapping =
                    sanityCheckAndGenerateIndexMapping(
                            toFlussSchema(upstreamCdcSchema), downStreamFlusstreamSchema);
        }
    }

    static Map<Integer, Integer> sanityCheckAndGenerateIndexMapping(
            com.alibaba.fluss.metadata.Schema inferredFlussSchema,
            com.alibaba.fluss.metadata.Schema currentFlussnewSchema) {
        List<String> inferredSchemaColumnNames = inferredFlussSchema.getColumnNames();
        Map<String, Integer> reverseIndex = new HashMap<>();
        for (int i = 0; i < inferredSchemaColumnNames.size(); i++) {
            reverseIndex.put(inferredSchemaColumnNames.get(i), i);
        }

        List<String> currentSchemaColumnNames = currentFlussnewSchema.getColumnNames();
        Map<Integer, Integer> indexMapping = new HashMap<>();
        for (int newSchemaIndex = 0;
                newSchemaIndex < currentSchemaColumnNames.size();
                newSchemaIndex++) {
            String columnName = currentSchemaColumnNames.get(newSchemaIndex);
            if (reverseIndex.get(columnName) != null) {
                Integer oldSchemaIndex = reverseIndex.get(columnName);
                indexMapping.put(newSchemaIndex, oldSchemaIndex);

                // Currently, we only support mismatched column counts between upstream and
                // downstream, but not mismatched data types, to prevent errors caused by type
                // changes.
                // In the future, meta applier will be used to handle column changes.
                DataType oldDataType = inferredFlussSchema.getRowType().getTypeAt(oldSchemaIndex);
                DataType newDataType = currentFlussnewSchema.getRowType().getTypeAt(newSchemaIndex);
                if (!oldDataType.copy(false).equals(newDataType.copy(false))) {
                    throw new IllegalArgumentException(
                            "The data type of column "
                                    + columnName
                                    + " is changed from "
                                    + oldDataType
                                    + " to "
                                    + newDataType);
                }
            }
        }
        return indexMapping;
    }
}
