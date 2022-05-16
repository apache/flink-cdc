/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.source;

import io.debezium.data.Envelope;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.math.BigInteger;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.Map;

/** Utils to deal with table schema of OceanBase. */
public class OceanBaseTableSchema {

    public static TableSchemaBuilder tableSchemaBuilder(ZoneOffset zoneOffset) {
        return new TableSchemaBuilder(
                OceanBaseJdbcConverter.valueConverterProvider(zoneOffset),
                SchemaNameAdjuster.create(),
                new CustomConverterRegistry(null),
                OceanBaseSchemaUtils.sourceSchema(),
                false);
    }

    public static TableId tableId(String databaseName, String tableName) {
        return new TableId(databaseName, null, tableName);
    }

    public static Column getColumn(String name, int jdbcType) {
        // we can't get the scale and length of decimal, timestamp and bit columns from log,
        // so here we set a constant value to these fields to be compatible with the logic of
        // JdbcValueConverters#schemaBuilder
        ColumnEditor columnEditor =
                Column.editor().name(name).jdbcType(jdbcType).optional(true).scale(0);
        if (columnEditor.jdbcType() == Types.TIMESTAMP || columnEditor.jdbcType() == Types.BIT) {
            columnEditor.length(6);
        }
        return columnEditor.create();
    }

    public static TableSchema getTableSchema(
            String topicName,
            String databaseName,
            String tableName,
            String[] columnNames,
            int[] jdbcTypes,
            ZoneOffset zoneOffset) {
        TableEditor tableEditor = Table.editor().tableId(tableId(databaseName, tableName));
        for (int i = 0; i < columnNames.length; i++) {
            tableEditor.addColumn(getColumn(columnNames[i], jdbcTypes[i]));
        }
        // TODO add column filter and mapper
        return tableSchemaBuilder(zoneOffset)
                .create(
                        null,
                        Envelope.schemaName(topicName),
                        tableEditor.create(),
                        null,
                        null,
                        null);
    }

    public static Schema upcastingSchemaType(Schema schema, String value) {
        if (schema.type().equals(Schema.Type.INT32) && Long.parseLong(value) > Integer.MAX_VALUE) {
            return Schema.INT64_SCHEMA;
        }
        if (schema.type().equals(Schema.Type.INT64)) {
            BigInteger bigInt = new BigInteger(value);
            if (bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                return Schema.STRING_SCHEMA;
            }
        }
        return schema;
    }

    public static Schema upcastingValueSchema(Schema valueSchema, Map<String, String> fields) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().optional();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            Schema fieldSchema = valueSchema.field(entry.getKey()).schema();
            fieldSchema = upcastingSchemaType(fieldSchema, entry.getValue());
            schemaBuilder.field(entry.getKey(), fieldSchema);
        }
        return schemaBuilder.build();
    }

    public static Envelope getEnvelope(String name, Schema valueSchema) {
        return Envelope.defineSchema()
                .withName(name)
                .withRecord(valueSchema)
                .withSource(OceanBaseSchemaUtils.sourceSchema())
                .build();
    }

    public static TableSchema upcastingTableSchema(
            String topicName, TableSchema tableSchema, Map<String, String> fields) {
        Schema valueSchema = upcastingValueSchema(tableSchema.valueSchema(), fields);
        return new TableSchema(
                tableSchema.id(),
                null,
                null,
                getEnvelope(Envelope.schemaName(topicName), valueSchema),
                valueSchema,
                null);
    }
}
