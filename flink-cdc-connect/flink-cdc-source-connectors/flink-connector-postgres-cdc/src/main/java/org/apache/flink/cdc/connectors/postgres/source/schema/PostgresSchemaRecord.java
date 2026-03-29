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

package org.apache.flink.cdc.connectors.postgres.source.schema;

import io.debezium.relational.Table;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** A SourceRecord wrapper carrying a Debezium Table for schema change propagation. */
public class PostgresSchemaRecord extends SourceRecord {
    // Use `postgres-cdc` rather than `postgres` to avoid conflict if debezium support later.
    private static final String SCHEMA_CHANGE_EVENT_KEY_NAME =
            "io.debezium.connector.postgres-cdc.SchemaChangeKey";
    private static final String SCHEMA_CHANGE_EVENT_VALUE_NAME =
            "io.debezium.connector.postgres-cdc.SchemaChangeValue";

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct()
                    .name(SCHEMA_CHANGE_EVENT_KEY_NAME)
                    .field("table_id", Schema.STRING_SCHEMA)
                    .build();

    /** Minimal value schema to avoid null valueSchema/value NPEs downstream. */
    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct().name(SCHEMA_CHANGE_EVENT_VALUE_NAME).build();

    private final Table table;

    public PostgresSchemaRecord(Table table) {
        super(
                null,
                null,
                null,
                KEY_SCHEMA,
                buildKey(table),
                VALUE_SCHEMA,
                new Struct(VALUE_SCHEMA));
        this.table = table;
    }

    private static Struct buildKey(Table table) {
        Struct struct = new Struct(KEY_SCHEMA);
        struct.put("table_id", table.id().toString());
        return struct;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "PostgresSchemaRecord{" + "table=" + table + '}';
    }
}
