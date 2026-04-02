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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.debezium.connector.AbstractSourceInfo.DATABASE_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleSourceFetchTaskContext} ROWID extraction. */
class OracleSourceFetchTaskContextTest {

    private static final String TABLE_TOPIC = "ORCLCDB.DEBEZIUM.CHUNK_KEY_NO_PK";
    private static final String LOWER_ROWID = "AAAzIdACKAAABWCAAA";
    private static final String UPPER_ROWID = "AAAzIdAC/AACWIPAAB";

    @Test
    void testExtractRowIdFromAfterStruct() {
        SourceRecord record = createDataChangeRecord(LOWER_ROWID, null, null);

        assertThat(OracleSourceFetchTaskContext.extractRowId(record))
                .isPresent()
                .get()
                .extracting(Object::toString)
                .isEqualTo(LOWER_ROWID);
    }

    @Test
    void testExtractRowIdFromBeforeStruct() {
        SourceRecord record = createDataChangeRecord(null, UPPER_ROWID, null);

        assertThat(OracleSourceFetchTaskContext.extractRowId(record))
                .isPresent()
                .get()
                .extracting(Object::toString)
                .isEqualTo(UPPER_ROWID);
    }

    @Test
    void testExtractRowIdFallsBackToHeaders() {
        SourceRecord record = createDataChangeRecord(null, null, LOWER_ROWID);

        assertThat(OracleSourceFetchTaskContext.extractRowId(record))
                .isPresent()
                .get()
                .extracting(Object::toString)
                .isEqualTo(LOWER_ROWID);
    }

    @Test
    void testExtractRowIdReturnsEmptyWhenMissing() {
        SourceRecord record = createDataChangeRecord(null, null, null);

        assertThat(OracleSourceFetchTaskContext.extractRowId(record)).isEmpty();
    }

    @Test
    void testOutputBufferKeyFallsBackToRowIdWhenRecordKeyIsMissing() {
        SourceRecord lowerRowIdRecord = createDataChangeRecord(LOWER_ROWID, null, null);
        SourceRecord upperRowIdRecord = createDataChangeRecord(UPPER_ROWID, null, null);

        assertThat(lowerRowIdRecord.key()).isNull();
        assertThat(upperRowIdRecord.key()).isNull();
        assertThat(OracleSourceFetchTaskContext.outputBufferKey(lowerRowIdRecord))
                .isEqualTo(LOWER_ROWID);
        assertThat(OracleSourceFetchTaskContext.outputBufferKey(upperRowIdRecord))
                .isEqualTo(UPPER_ROWID);
    }

    @Test
    void testOutputBufferKeyFallsBackToRowIdWhenRecordKeyIsEmptyStruct() {
        Schema keySchema = SchemaBuilder.struct().optional().build();
        Struct emptyKey = new Struct(keySchema);
        SourceRecord lowerRowIdRecord =
                createDataChangeRecord(LOWER_ROWID, null, null, keySchema, emptyKey);
        SourceRecord upperRowIdRecord =
                createDataChangeRecord(UPPER_ROWID, null, null, keySchema, emptyKey);

        assertThat(lowerRowIdRecord.key()).isEqualTo(emptyKey);
        assertThat(upperRowIdRecord.key()).isEqualTo(emptyKey);
        assertThat(OracleSourceFetchTaskContext.outputBufferKey(lowerRowIdRecord))
                .isEqualTo(LOWER_ROWID);
        assertThat(OracleSourceFetchTaskContext.outputBufferKey(upperRowIdRecord))
                .isEqualTo(UPPER_ROWID);
    }

    private SourceRecord createDataChangeRecord(
            String afterRowId, String beforeRowId, String headerRowId) {
        return createDataChangeRecord(afterRowId, beforeRowId, headerRowId, null, null);
    }

    private SourceRecord createDataChangeRecord(
            String afterRowId,
            String beforeRowId,
            String headerRowId,
            Schema keySchema,
            Object keyValue) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .build();
        Schema rowSchema =
                SchemaBuilder.struct()
                        .field("ID", Schema.INT32_SCHEMA)
                        .field(ROWID.class.getSimpleName(), Schema.OPTIONAL_STRING_SCHEMA)
                        .optional()
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field("op", Schema.STRING_SCHEMA)
                        .field("source", sourceSchema)
                        .field("before", rowSchema)
                        .field("after", rowSchema)
                        .build();

        Struct value =
                new Struct(valueSchema)
                        .put("op", "r")
                        .put(
                                "source",
                                new Struct(sourceSchema)
                                        .put(DATABASE_NAME_KEY, "ORCLCDB")
                                        .put(SCHEMA_NAME_KEY, "DEBEZIUM")
                                        .put(TABLE_NAME_KEY, "CHUNK_KEY_NO_PK"));
        if (beforeRowId != null) {
            value.put(
                    "before",
                    new Struct(rowSchema)
                            .put("ID", 1)
                            .put(ROWID.class.getSimpleName(), beforeRowId));
        }
        if (afterRowId != null) {
            value.put(
                    "after",
                    new Struct(rowSchema)
                            .put("ID", 1)
                            .put(ROWID.class.getSimpleName(), afterRowId));
        }

        ConnectHeaders headers = new ConnectHeaders();
        if (headerRowId != null) {
            headers.add(ROWID.class.getSimpleName(), headerRowId, null);
        }

        return new SourceRecord(
                Collections.singletonMap("server", "oracle"),
                Collections.emptyMap(),
                TABLE_TOPIC,
                null,
                keySchema,
                keyValue,
                valueSchema,
                value,
                null,
                headers);
    }
}
