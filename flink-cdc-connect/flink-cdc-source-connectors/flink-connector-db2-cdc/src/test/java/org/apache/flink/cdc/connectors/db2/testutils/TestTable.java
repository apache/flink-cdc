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

package org.apache.flink.cdc.connectors.db2.testutils;

import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * Test utility for creating converter, formatter and deserializer of a table in the test database.
 */
public class TestTable {

    private final String databaseName;
    private final String tableName;

    private final String schemaName;

    private final ResolvedSchema schema;

    // Lazily initialized components
    private RowRowConverter rowRowConverter;
    private RowDataDebeziumDeserializeSchema deserializer;
    private RecordsFormatter recordsFormatter;

    public TestTable(
            String databaseName, String schemaName, String tableName, ResolvedSchema schema) {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.schema = schema;
    }

    public String getTableId() {
        return String.format("%s.%s.%s", databaseName, schemaName, tableName);
    }

    public RowType getRowType() {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    public RowDataDebeziumDeserializeSchema getDeserializer() {
        if (deserializer == null) {
            deserializer =
                    RowDataDebeziumDeserializeSchema.newBuilder()
                            .setPhysicalRowType(getRowType())
                            .setResultTypeInfo(InternalTypeInfo.of(getRowType()))
                            .build();
        }
        return deserializer;
    }

    public RowRowConverter getRowRowConverter() {
        if (rowRowConverter == null) {
            rowRowConverter = RowRowConverter.create(schema.toPhysicalRowDataType());
        }
        return rowRowConverter;
    }

    public RecordsFormatter getRecordsFormatter() {
        if (recordsFormatter == null) {
            recordsFormatter = new RecordsFormatter(schema.toPhysicalRowDataType());
        }
        return recordsFormatter;
    }

    public String stringify(RowData rowData) {
        return getRowRowConverter().toExternal(rowData).toString();
    }

    public List<String> stringify(List<SourceRecord> sourceRecord) {
        return getRecordsFormatter().format(sourceRecord);
    }
}
