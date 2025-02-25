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

package org.apache.flink.cdc.connectors.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;

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
            Assertions.assertThat(record.key()).isNotNull();
            Assertions.assertThat(record.keySchema()).isNotNull();
        } else {
            Assertions.assertThat(record.key()).isNull();
            Assertions.assertThat(record.keySchema()).isNull();
        }

        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString(Envelope.FieldName.OPERATION))
                .isEqualTo(Envelope.Operation.CREATE.code());
        Assertions.assertThat(value.get(Envelope.FieldName.AFTER)).isNotNull();
        Assertions.assertThat(value.get(Envelope.FieldName.BEFORE)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#READ READ} record.
     *
     * @param record the source record; may not be null
     */
    public static void assertRead(SourceRecord record) {
        Assertions.assertThat(record.key()).isNotNull();
        Assertions.assertThat(record.keySchema()).isNotNull();
        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString(Envelope.FieldName.OPERATION))
                .isEqualTo(Envelope.Operation.READ.code());
        Assertions.assertThat(value.get(Envelope.FieldName.AFTER)).isNotNull();
        Assertions.assertThat(value.get(Envelope.FieldName.BEFORE)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#UPDATE UPDATE}
     * record.
     *
     * @param record the source record; may not be null
     */
    public static void assertUpdate(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            Assertions.assertThat(record.key()).isNotNull();
            Assertions.assertThat(record.keySchema()).isNotNull();
        } else {
            Assertions.assertThat(record.key()).isNull();
            Assertions.assertThat(record.keySchema()).isNull();
        }
        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString(Envelope.FieldName.OPERATION))
                .isEqualTo(Envelope.Operation.UPDATE.code());
        Assertions.assertThat(value.get(Envelope.FieldName.AFTER)).isNotNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Envelope.Operation#DELETE DELETE}
     * record.
     *
     * @param record the source record; may not be null
     */
    public static void assertDelete(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            Assertions.assertThat(record.key()).isNotNull();
            Assertions.assertThat(record.keySchema()).isNotNull();
        } else {
            Assertions.assertThat(record.key()).isNull();
            Assertions.assertThat(record.keySchema()).isNull();
        }
        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString(Envelope.FieldName.OPERATION))
                .isEqualTo(Envelope.Operation.DELETE.code());
        Assertions.assertThat(value.get(Envelope.FieldName.BEFORE)).isNotNull();
        Assertions.assertThat(value.get(Envelope.FieldName.AFTER)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a valid tombstone, meaning it has a non-null
     * key and key schema but null value and value schema.
     *
     * @param record the source record; may not be null
     */
    public static void assertTombstone(SourceRecord record) {
        Assertions.assertThat(record.key()).isNotNull();
        Assertions.assertThat(record.keySchema()).isNotNull();
        Assertions.assertThat(record.value()).isNull();
        Assertions.assertThat(record.valueSchema()).isNull();
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
        Assertions.assertThat(key.get(pkField)).isEqualTo(pk);
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
        Assertions.assertThat(producedType).isExactlyInstanceOf(InternalTypeInfo.class);
        InternalTypeInfo<RowData> rowDataInternalTypeInfo =
                (InternalTypeInfo<RowData>) producedType;
        DataType producedDataType = rowDataInternalTypeInfo.getDataType();
        Assertions.assertThat(producedDataType).hasToString(expectedProducedType.toString());
    }
}
