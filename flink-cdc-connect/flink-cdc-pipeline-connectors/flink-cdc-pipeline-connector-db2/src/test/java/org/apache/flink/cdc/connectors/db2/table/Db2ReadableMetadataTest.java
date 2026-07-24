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

package org.apache.flink.cdc.connectors.db2.table;

import org.apache.flink.table.data.TimestampData;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class Db2ReadableMetadataTest {

    @Test
    void testReadDatabaseSchemaTableAndOperationTimestamp() {
        SourceRecord record = sourceRecord("TESTDB", "DB2INST1", "PRODUCTS", 1_714_000_000_123L);

        assertThat(Db2ReadableMetadata.DATABASE_NAME.getConverter().read(record).toString())
                .isEqualTo("TESTDB");
        assertThat(Db2ReadableMetadata.SCHEMA_NAME.getConverter().read(record).toString())
                .isEqualTo("DB2INST1");
        assertThat(Db2ReadableMetadata.TABLE_NAME.getConverter().read(record).toString())
                .isEqualTo("PRODUCTS");
        assertThat(
                        ((TimestampData) Db2ReadableMetadata.OP_TS.getConverter().read(record))
                                .getMillisecond())
                .isEqualTo(1_714_000_000_123L);
    }

    @Test
    void testMetadataKeysAndTypes() {
        assertThat(Db2ReadableMetadata.DATABASE_NAME.getKey()).isEqualTo("database_name");
        assertThat(Db2ReadableMetadata.SCHEMA_NAME.getKey()).isEqualTo("schema_name");
        assertThat(Db2ReadableMetadata.TABLE_NAME.getKey()).isEqualTo("table_name");
        assertThat(Db2ReadableMetadata.OP_TS.getKey()).isEqualTo("op_ts");
        assertThat(Db2ReadableMetadata.OP_TS.getDataType().toString())
                .isEqualTo("TIMESTAMP_LTZ(3) NOT NULL");
    }

    private static SourceRecord sourceRecord(
            String database, String schema, String table, long timestampMillis) {
        org.apache.kafka.connect.data.Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("source")
                        .field(
                                AbstractSourceInfo.DATABASE_NAME_KEY,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .field(
                                AbstractSourceInfo.SCHEMA_NAME_KEY,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .field(
                                AbstractSourceInfo.TABLE_NAME_KEY,
                                org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
                        .field(
                                AbstractSourceInfo.TIMESTAMP_KEY,
                                org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                        .build();
        Struct sourceStruct =
                new Struct(sourceSchema)
                        .put(AbstractSourceInfo.DATABASE_NAME_KEY, database)
                        .put(AbstractSourceInfo.SCHEMA_NAME_KEY, schema)
                        .put(AbstractSourceInfo.TABLE_NAME_KEY, table)
                        .put(AbstractSourceInfo.TIMESTAMP_KEY, timestampMillis);

        org.apache.kafka.connect.data.Schema valueSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.db2.Envelope")
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .build();
        Struct valueStruct = new Struct(valueSchema).put(Envelope.FieldName.SOURCE, sourceStruct);

        return new SourceRecord(
                Collections.singletonMap("server", "server1"),
                Collections.singletonMap("lsn", "1"),
                "server1.TESTDB.DB2INST1.PRODUCTS",
                null,
                null,
                null,
                valueSchema,
                valueStruct);
    }
}
