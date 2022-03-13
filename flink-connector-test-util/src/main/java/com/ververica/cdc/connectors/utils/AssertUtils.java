/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Utilities for asserting {@link SourceRecord} and {@link DebeziumSourceFunction}. */
public class AssertUtils {
    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#CREATE
     * INSERT/CREATE} record.
     *
     * @param record the source record; may not be null
     */
    public static void assertInsert(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertNotNull(record.key());
            assertNotNull(record.keySchema());
        } else {
            assertNull(record.key());
            assertNull(record.keySchema());
        }

        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(
                Envelope.Operation.CREATE.code(), value.getString(Envelope.FieldName.OPERATION));
        assertNotNull(value.get(Envelope.FieldName.AFTER));
        assertNull(value.get(Envelope.FieldName.BEFORE));
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#READ READ} record.
     *
     * @param record the source record; may not be null
     */
    public static void assertRead(SourceRecord record) {
        assertNotNull(record.key());
        assertNotNull(record.keySchema());
        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(Envelope.Operation.READ.code(), value.getString(Envelope.FieldName.OPERATION));
        assertNotNull(value.get(Envelope.FieldName.AFTER));
        assertNull(value.get(Envelope.FieldName.BEFORE));
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#UPDATE UPDATE}
     * record.
     *
     * @param record the source record; may not be null
     */
    public static void assertUpdate(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertNotNull(record.key());
            assertNotNull(record.keySchema());
        } else {
            assertNull(record.key());
            assertNull(record.keySchema());
        }
        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(
                Envelope.Operation.UPDATE.code(), value.getString(Envelope.FieldName.OPERATION));
        assertNotNull(value.get(Envelope.FieldName.AFTER));
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#DELETE DELETE}
     * record.
     *
     * @param record the source record; may not be null
     */
    public static void assertDelete(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertNotNull(record.key());
            assertNotNull(record.keySchema());
        } else {
            assertNull(record.key());
            assertNull(record.keySchema());
        }
        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(
                Envelope.Operation.DELETE.code(), value.getString(Envelope.FieldName.OPERATION));
        assertNotNull(value.get(Envelope.FieldName.BEFORE));
        assertNull(value.get(Envelope.FieldName.AFTER));
    }

    /**
     * Verify that the given {@link SourceRecord} is a valid tombstone, meaning it has a non-null
     * key and key schema but null value and value schema.
     *
     * @param record the source record; may not be null
     */
    public static void assertTombstone(SourceRecord record) {
        assertNotNull(record.key());
        assertNotNull(record.keySchema());
        assertNull(record.value());
        assertNull(record.valueSchema());
    }

    /**
     * Verify that the given {@link SourceRecord} has a valid non-null integer key that matches the
     * expected integer value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void hasValidKey(SourceRecord record, String pkField, int pk) {
        Struct key = (Struct) record.key();
        assertEquals(pk, key.get(pkField));
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#CREATE
     * INSERT/CREATE} record without primary key.
     *
     * @param record the source record; may not be null
     */
    public static void assertInsert(SourceRecord record) {
        assertInsert(record, false);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#CREATE
     * INSERT/CREATE} record, and that the integer key matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void assertInsert(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        assertInsert(record, true);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#CREATE READ}
     * record, and that the integer key matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void assertRead(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        assertRead(record);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#UPDATE UPDATE}
     * record without PK.
     *
     * @param record the source record; may not be null
     */
    public static void assertUpdate(SourceRecord record) {
        assertUpdate(record, false);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#UPDATE UPDATE}
     * record, and that the integer key matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void assertUpdate(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        assertUpdate(record, true);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#DELETE DELETE}
     * record without PK. matches the expected value.
     *
     * @param record the source record; may not be null
     */
    public static void assertDelete(SourceRecord record) {
        assertDelete(record, false);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#DELETE DELETE}
     * record, and that the integer key matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void assertDelete(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        assertDelete(record, true);
    }

    /**
     * Verify that the given produced data type of {@code DebeziumSourceFunction<RowData>} matches
     * the resolved schema data type.
     *
     * @param debeziumSourceFunction the actual DebeziumSourceFunction
     * @param expectedProducedType expected DataType of resolved schema
     */
    public static void assertProducedTypeOfSourceFunction(
            DebeziumSourceFunction<RowData> debeziumSourceFunction, DataType expectedProducedType) {
        TypeInformation<RowData> producedType = debeziumSourceFunction.getProducedType();
        assertThat(producedType, instanceOf(InternalTypeInfo.class));
        InternalTypeInfo<RowData> rowDataInternalTypeInfo =
                (InternalTypeInfo<RowData>) producedType;
        DataType producedDataType = rowDataInternalTypeInfo.getDataType();
        assertEquals(expectedProducedType.toString(), producedDataType.toString());
    }
}
