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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.time.format.DateTimeParseException;

/** Unit tests for the {@link PostTransformOperator}. */
class PostTransformOperatorTest {
    private static final TableId CUSTOMERS_TABLEID =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("col2", DataTypes.STRING())
                    .physicalColumn("col12", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();

    private static final TableId DATATYPE_TABLEID =
            TableId.tableId("my_company", "my_branch", "data_types");
    private static final Schema DATATYPE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("colString", DataTypes.STRING().notNull())
                    .physicalColumn("colBoolean", DataTypes.BOOLEAN())
                    .physicalColumn("colTinyint", DataTypes.TINYINT())
                    .physicalColumn("colSmallint", DataTypes.SMALLINT())
                    .physicalColumn("colInt", DataTypes.INT())
                    .physicalColumn("colBigint", DataTypes.BIGINT())
                    .physicalColumn("colDate", DataTypes.DATE())
                    .physicalColumn("colTime", DataTypes.TIME())
                    .physicalColumn("colTimestamp", DataTypes.TIMESTAMP())
                    .physicalColumn("colFloat", DataTypes.FLOAT())
                    .physicalColumn("colDouble", DataTypes.DOUBLE())
                    .physicalColumn("colDecimal", DataTypes.DECIMAL(6, 2))
                    .primaryKey("colString")
                    .build();

    private static final TableId METADATA_TABLEID =
            TableId.tableId("my_company", "my_branch", "metadata_table");
    private static final Schema METADATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECTED_METADATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("identifier_name", DataTypes.STRING())
                    .physicalColumn("__namespace_name__", DataTypes.STRING().notNull())
                    .physicalColumn("__schema_name__", DataTypes.STRING().notNull())
                    .physicalColumn("__table_name__", DataTypes.STRING().notNull())
                    .primaryKey("col1")
                    .build();

    private static final TableId METADATA_AS_TABLEID =
            TableId.tableId("my_company", "my_branch", "metadata_as_table");
    private static final Schema METADATA_AS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("sid", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("name_upper", DataTypes.STRING())
                    .physicalColumn("tbname", DataTypes.STRING().notNull())
                    .physicalColumn("tbname_sid", DataTypes.STRING())
                    .physicalColumn("sid_tbname", DataTypes.STRING())
                    .physicalColumn("tbname_name", DataTypes.STRING())
                    .physicalColumn("name_tbname", DataTypes.STRING())
                    .primaryKey("sid")
                    .build();

    private static final TableId TIMESTAMP_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestamp_table");
    private static final Schema TIMESTAMP_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("time_equal", DataTypes.INT())
                    .physicalColumn("timestamp_equal", DataTypes.INT())
                    .physicalColumn("date_equal", DataTypes.INT())
                    .primaryKey("col1")
                    .build();

    private static final TableId FROM_UNIX_TIME_TABLEID =
            TableId.tableId("my_company", "my_branch", "from_unix_time_table");
    private static final Schema FROM_UNIX_TIME_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("seconds", DataTypes.BIGINT())
                    .physicalColumn("format_str", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECTED_FROM_UNIX_TIME_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("from_unix_time", DataTypes.STRING())
                    .physicalColumn("from_unix_time_format", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();

    private static final TableId UNIX_TIMESTAMP_TABLEID =
            TableId.tableId("my_company", "my_branch", "unix_timestamp_table");
    private static final Schema UNIX_TIMESTAMP_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("date_time_str", DataTypes.STRING())
                    .physicalColumn("unix_timestamp_format", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECTED_UNIX_TIMESTAMP_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("unix_timestamp", DataTypes.BIGINT())
                    .physicalColumn("unix_timestamp_format", DataTypes.BIGINT())
                    .primaryKey("col1")
                    .build();

    private static final TableId TIMESTAMPDIFF_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestampdiff_table");
    private static final Schema TIMESTAMPDIFF_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("second_diff", DataTypes.INT())
                    .physicalColumn("minute_diff", DataTypes.INT())
                    .physicalColumn("hour_diff", DataTypes.INT())
                    .physicalColumn("day_diff", DataTypes.INT())
                    .physicalColumn("month_diff", DataTypes.INT())
                    .physicalColumn("year_diff", DataTypes.INT())
                    .primaryKey("col1")
                    .build();

    private static final TableId TIMESTAMPDIFF_DATA_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestampdiff_data_table");
    private static final Schema TIMESTAMPDIFF_DATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING())
                    .physicalColumn("time_interval_unit", DataTypes.STRING())
                    .physicalColumn("start_timestamp", DataTypes.TIMESTAMP())
                    .physicalColumn("end_timestamp", DataTypes.TIMESTAMP())
                    .physicalColumn("start_timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                    .physicalColumn("end_timestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                    .primaryKey("col1")
                    .build();
    private static final Schema EXPECTED_TIMESTAMPDIFF_DATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("time_interval_unit", DataTypes.STRING())
                    .physicalColumn("timestamp_timestamp", DataTypes.INT())
                    .physicalColumn("timestamp_timestamp_ltz", DataTypes.INT())
                    .physicalColumn("timestamp_ltz_timestamp", DataTypes.INT())
                    .physicalColumn("timestamp_ltz_timestamp_ltz", DataTypes.INT())
                    .primaryKey("col1")
                    .build();

    private static final TableId TIMESTAMPADD_DATA_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestampadd_data_table");
    private static final Schema TIMESTAMPADD_DATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("time_interval_unit", DataTypes.STRING())
                    .physicalColumn("interval_value", DataTypes.INT())
                    .physicalColumn("time_point_timestamp", DataTypes.TIMESTAMP(0))
                    .physicalColumn("time_point_timestamp_ltz", DataTypes.TIMESTAMP_LTZ(0))
                    .primaryKey("col1")
                    .build();

    private static final TableId TIMESTAMPADD_TABLEID =
            TableId.tableId("my_company", "my_branch", "timestampadd_table");
    private static final Schema TIMESTAMPADD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("second_add", DataTypes.STRING())
                    .physicalColumn("minute_add", DataTypes.STRING())
                    .physicalColumn("hour_add", DataTypes.STRING())
                    .physicalColumn("day_add", DataTypes.STRING())
                    .physicalColumn("month_add", DataTypes.STRING())
                    .physicalColumn("year_add", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();

    private static final TableId NULL_TABLEID =
            TableId.tableId("my_company", "my_branch", "data_null");
    private static final Schema NULL_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("colString", DataTypes.STRING())
                    .physicalColumn("nullInt", DataTypes.INT())
                    .physicalColumn("nullBoolean", DataTypes.BOOLEAN())
                    .physicalColumn("nullTinyint", DataTypes.TINYINT())
                    .physicalColumn("nullSmallint", DataTypes.SMALLINT())
                    .physicalColumn("nullBigint", DataTypes.BIGINT())
                    .physicalColumn("nullFloat", DataTypes.FLOAT())
                    .physicalColumn("nullDouble", DataTypes.DOUBLE())
                    .physicalColumn("nullChar", DataTypes.STRING())
                    .physicalColumn("nullVarchar", DataTypes.STRING())
                    .physicalColumn("nullDecimal", DataTypes.DECIMAL(4, 2))
                    .physicalColumn("nullTimestamp", DataTypes.TIMESTAMP(3))
                    .primaryKey("col1")
                    .build();

    private static final TableId CAST_TABLEID =
            TableId.tableId("my_company", "my_branch", "data_cast");
    private static final Schema CAST_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("castInt", DataTypes.INT())
                    .physicalColumn("castBoolean", DataTypes.BOOLEAN())
                    .physicalColumn("castTinyint", DataTypes.TINYINT())
                    .physicalColumn("castSmallint", DataTypes.SMALLINT())
                    .physicalColumn("castBigint", DataTypes.BIGINT())
                    .physicalColumn("castFloat", DataTypes.FLOAT())
                    .physicalColumn("castDouble", DataTypes.DOUBLE())
                    .physicalColumn("castChar", DataTypes.STRING())
                    .physicalColumn("castVarchar", DataTypes.STRING())
                    .physicalColumn("castDecimal", DataTypes.DECIMAL(4, 2))
                    .physicalColumn("castTimestamp", DataTypes.TIMESTAMP(3))
                    .primaryKey("col1")
                    .build();

    private static final TableId TIMEZONE_TABLEID =
            TableId.tableId("my_company", "my_branch", "timezone_table");
    private static final Schema TIMEZONE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("datetime_value", DataTypes.STRING())
                    .primaryKey("col1")
                    .build();

    private static final TableId CONDITION_TABLEID =
            TableId.tableId("my_company", "my_branch", "condition_table");
    private static final Schema CONDITION_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("condition_result", DataTypes.BOOLEAN())
                    .primaryKey("col1")
                    .build();

    private static final TableId REDUCE_TABLEID =
            TableId.tableId("my_company", "my_branch", "reduce_table");

    private static final Schema REDUCE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("ref1", DataTypes.STRING())
                    .physicalColumn("ref2", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_REDUCE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("uid", DataTypes.STRING())
                    .physicalColumn("newage", DataTypes.INT())
                    .physicalColumn("ref1", DataTypes.STRING())
                    .physicalColumn("seventeen", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final TableId WILDCARD_TABLEID =
            TableId.tableId("my_company", "my_branch", "wildcard_table");

    private static final Schema WILDCARD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final Schema EXPECTED_WILDCARD_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING().notNull())
                    .physicalColumn("age", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("newage", DataTypes.INT())
                    .primaryKey("id")
                    .partitionKey("id")
                    .options(ImmutableMap.of("key1", "value1", "key2", "value2"))
                    .build();

    private static final TableId COLUMN_SQUARE_TABLE =
            TableId.tableId("my_company", "my_branch", "column_square");
    private static final Schema COLUMN_SQUARE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.INT().notNull())
                    .physicalColumn("col2", DataTypes.INT())
                    .physicalColumn("square_col2", DataTypes.INT())
                    .primaryKey("col1")
                    .build();

    private static final TableId COMPARE_TABLEID =
            TableId.tableId("my_company", "my_branch", "compare_table");
    private static final Schema COMPARE_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.STRING().notNull())
                    .physicalColumn("numerical_equal", DataTypes.BOOLEAN())
                    .physicalColumn("string_equal", DataTypes.BOOLEAN())
                    .physicalColumn("time_equal", DataTypes.BOOLEAN())
                    .physicalColumn("timestamp_equal", DataTypes.BOOLEAN())
                    .physicalColumn("date_equal", DataTypes.BOOLEAN())
                    .primaryKey("col1")
                    .build();

    private static final TableId COMPARE_DATA_TABLEID =
            TableId.tableId("my_company", "my_branch", "compare_data_table");
    private static final Schema COMPARE_DATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("c1", DataTypes.FLOAT().nullable())
                    .physicalColumn("c2", DataTypes.DOUBLE().nullable())
                    .physicalColumn("c3", DataTypes.TIMESTAMP().nullable())
                    .primaryKey("id")
                    .build();
    private static final Schema EXPECTD_COMPARE_DATA_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("float_equal", DataTypes.BOOLEAN())
                    .physicalColumn("double_equal", DataTypes.BOOLEAN())
                    .physicalColumn("timestamp_equal", DataTypes.BOOLEAN())
                    .primaryKey("id")
                    .build();

    private static final TableId COL_NAME_MAPPING_TABLEID =
            TableId.tableId("my_company", "my_branch", "col_name_mapping_table");
    private static final Schema COL_NAME_MAPPING_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("foo", DataTypes.INT())
                    .physicalColumn("bar", DataTypes.INT())
                    .physicalColumn("foo-bar", DataTypes.INT())
                    .physicalColumn("bar-foo", DataTypes.INT())
                    .physicalColumn("class", DataTypes.STRING())
                    .physicalColumn("f0", DataTypes.INT())
                    .physicalColumn("f1", DataTypes.INT())
                    .physicalColumn("f2", DataTypes.INT())
                    .build();

    @Test
    void testDataChangeEventTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1,col2) col12",
                                "col1 = '1'")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("12")
                                }));
        // Insert will be ignored
        DataChangeEvent insertEventIgnored =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"), new BinaryStringData("2"), null
                                }));
        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("3"), null
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("12")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
                                    new BinaryStringData("13")
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testDataChangeEventTransformTwice() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1, '1') col12",
                                "col1 = '1'")
                        .addTransform(
                                CUSTOMERS_TABLEID.identifier(),
                                "*, concat(col1, '2') col12",
                                "col1 = '2'")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CUSTOMERS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("11")
                                }));
        // Insert
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"), new BinaryStringData("2"), null
                                }));
        DataChangeEvent insertEvent2Expect =
                DataChangeEvent.insertEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("22")
                                }));
        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("2"), null
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), new BinaryStringData("3"), null
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        CUSTOMERS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2"),
                                    new BinaryStringData("11")
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("3"),
                                    new BinaryStringData("11")
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CUSTOMERS_TABLEID, CUSTOMERS_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEvent2Expect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testDataChangeEventTransformProjectionDataTypeConvert() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                DATATYPE_TABLEID.identifier(),
                                "*",
                                null,
                                null,
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(DATATYPE_TABLEID, DATATYPE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) DATATYPE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        DATATYPE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("3.14"),
                                    true,
                                    (byte) 1,
                                    (short) 1,
                                    1,
                                    1L,
                                    DateData.fromEpochDay(1704471599),
                                    TimeData.fromMillisOfDay(1704471),
                                    TimestampData.fromMillis(1704471599),
                                    3.14f,
                                    3.14d,
                                    DecimalData.fromBigDecimal(new BigDecimal("3.14"), 6, 2),
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(DATATYPE_TABLEID, DATATYPE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEvent));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testMetadataTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                METADATA_TABLEID.identifier(),
                                "*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name, __namespace_name__, __schema_name__, __table_name__",
                                " __table_name__ = 'metadata_table' ")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(METADATA_TABLEID, METADATA_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) METADATA_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_METADATA_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        recordDataGenerator.generate(new Object[] {new BinaryStringData("1")}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("my_company.my_branch.metadata_table"),
                                    new BinaryStringData("my_company"),
                                    new BinaryStringData("my_branch"),
                                    new BinaryStringData("metadata_table")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(METADATA_TABLEID, EXPECTED_METADATA_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testMetadataASTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                METADATA_AS_TABLEID.identifier(),
                                "sid, name, UPPER(name) as name_upper, __table_name__ as tbname, "
                                        + "concat(__table_name__,'_',sid) as tbname_sid, concat(sid,'_',__table_name__) as sid_tbname,"
                                        + "concat(__table_name__,'_',name) as tbname_name, concat(name,'_',__table_name__) as name_tbname",
                                "sid < 3")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(METADATA_AS_TABLEID, METADATA_AS_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) METADATA_AS_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        METADATA_AS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1,
                                    new BinaryStringData("abc"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        METADATA_AS_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1,
                                    new BinaryStringData("abc"),
                                    new BinaryStringData("ABC"),
                                    new BinaryStringData("metadata_as_table"),
                                    new BinaryStringData("metadata_as_table_1"),
                                    new BinaryStringData("1_metadata_as_table"),
                                    new BinaryStringData("metadata_as_table_abc"),
                                    new BinaryStringData("abc_metadata_as_table")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(METADATA_AS_TABLEID, METADATA_AS_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testSoftDeleteTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                METADATA_TABLEID.identifier(),
                                "*, __namespace_name__ || '.' || __schema_name__ || '.' || __table_name__ identifier_name, __namespace_name__, __schema_name__, __table_name__, __data_event_type__",
                                " __table_name__ = 'metadata_table' ",
                                "",
                                "",
                                "",
                                "SOFT_DELETE",
                                new SupportedMetadataColumn[0])
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        Schema expectedSchema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING().notNull())
                        .physicalColumn("identifier_name", DataTypes.STRING())
                        .physicalColumn("__namespace_name__", DataTypes.STRING().notNull())
                        .physicalColumn("__schema_name__", DataTypes.STRING().notNull())
                        .physicalColumn("__table_name__", DataTypes.STRING().notNull())
                        .physicalColumn("__data_event_type__", DataTypes.STRING().notNull())
                        .primaryKey("col1")
                        .build();

        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(METADATA_TABLEID, METADATA_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) METADATA_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) expectedSchema.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        recordDataGenerator.generate(new Object[] {new BinaryStringData("1")}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("my_company.my_branch.metadata_table"),
                                    new BinaryStringData("my_company"),
                                    new BinaryStringData("my_branch"),
                                    new BinaryStringData("metadata_table"),
                                    new BinaryStringData("+I")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(new CreateTableEvent(METADATA_TABLEID, expectedSchema)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        // Delete
        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        METADATA_TABLEID,
                        recordDataGenerator.generate(new Object[] {new BinaryStringData("1")}));
        DataChangeEvent deleteEventExpect =
                DataChangeEvent.insertEvent(
                        METADATA_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("my_company.my_branch.metadata_table"),
                                    new BinaryStringData("my_company"),
                                    new BinaryStringData("my_branch"),
                                    new BinaryStringData("metadata_table"),
                                    new BinaryStringData("-D")
                                }));
        transform.processElement(new StreamRecord<>(deleteEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(deleteEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testDataChangeEventTransformWithDuplicateColumns() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                COLUMN_SQUARE_TABLE.identifier(),
                                "col1, col2, col2 * col2 as square_col2",
                                "col2 < 3 OR col2 > 5")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(COLUMN_SQUARE_TABLE, COLUMN_SQUARE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) COLUMN_SQUARE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        COLUMN_SQUARE_TABLE,
                        recordDataGenerator.generate(new Object[] {1, 1, null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        COLUMN_SQUARE_TABLE, recordDataGenerator.generate(new Object[] {1, 1, 1}));

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        COLUMN_SQUARE_TABLE,
                        recordDataGenerator.generate(new Object[] {6, 6, null}));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        COLUMN_SQUARE_TABLE, recordDataGenerator.generate(new Object[] {6, 6, 36}));

        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        COLUMN_SQUARE_TABLE,
                        recordDataGenerator.generate(new Object[] {4, 4, null}));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(COLUMN_SQUARE_TABLE, COLUMN_SQUARE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));
        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isNull();
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testTimestampTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMP_TABLEID.identifier(),
                                "col1, IF(LOCALTIME = CURRENT_TIME, 1, 0) as time_equal,"
                                        + " IF(DATE_FORMAT(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') = DATE_FORMAT(CAST(NOW() AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss'), 1, 0) as timestamp_equal,"
                                        + " IF(TO_DATE(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd')) = CURRENT_DATE, 1, 0) as date_equal",
                                "LOCALTIMESTAMP = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)")
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMP_TABLEID, TIMESTAMP_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMESTAMP_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), null, null, null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), 1, 1, 1}));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(TIMESTAMP_TABLEID, TIMESTAMP_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testFromUnixTimeTransform() throws Exception {
        // In UTC, from_unix_time(0s) ==> 1970-01-01 00:00:00
        testFromUnixTimeTransformWithTimeZone("UTC", 0L, "1970-01-01 00:00:00");
        // In UTC, from_unix_time(44s) ==> 1970-01-01 00:00:44
        testFromUnixTimeTransformWithTimeZone("UTC", 44L, "1970-01-01 00:00:44");
        // In Berlin, the time zone is +1:00, from_unix_time(44s) ==> 1970-01-01 01:00:44
        testFromUnixTimeTransformWithTimeZone("Europe/Berlin", 44L, "1970-01-01 01:00:44");
        // In Shanghai, the time zone is +8:00, from_unix_time(44s) ==> 1970-01-01 08:00:44
        testFromUnixTimeTransformWithTimeZone("Asia/Shanghai", 44L, "1970-01-01 08:00:44");
    }

    private void testFromUnixTimeTransformWithTimeZone(
            String timeZone, Long seconds, String unixTimeStr) throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                FROM_UNIX_TIME_TABLEID.identifier(),
                                "col1, FROM_UNIXTIME(seconds) as from_unix_time,"
                                        + " FROM_UNIXTIME(seconds, format_str) as from_unix_time_format",
                                null)
                        .addTimezone(timeZone)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(FROM_UNIX_TIME_TABLEID, FROM_UNIX_TIME_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) FROM_UNIX_TIME_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        ((RowType) EXPECTED_FROM_UNIX_TIME_SCHEMA.toRowDataType()));
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        FROM_UNIX_TIME_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    seconds,
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss")
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        FROM_UNIX_TIME_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData(unixTimeStr),
                                    new BinaryStringData(unixTimeStr)
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        FROM_UNIX_TIME_TABLEID, EXPECTED_FROM_UNIX_TIME_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    /*
    Converts a date time string string1 with format string2 (by default: yyyy-MM-dd HH:mm:ss if not specified) to Unix timestamp (in seconds),
    using the specified timezone in table config.

    If a time zone is specified in the date time string and parsed by UTC+X format such as “yyyy-MM-dd HH:mm:ss.SSS X”,
    this function will use the specified timezone in the date time string instead of the timezone in table config. If the date time string can not be parsed,
    the default value Long.MIN_VALUE(-9223372036854775808) will be returned.
    */
    @Test
    void testUnixTimestampTransformInBerlin() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                UNIX_TIMESTAMP_TABLEID.identifier(),
                                "col1,"
                                        + " UNIX_TIMESTAMP(date_time_str) as unix_timestamp,"
                                        + " UNIX_TIMESTAMP(date_time_str, unix_timestamp_format) as unix_timestamp_format",
                                null)
                        .addTimezone("Europe/Berlin")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(UNIX_TIMESTAMP_TABLEID, UNIX_TIMESTAMP_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) UNIX_TIMESTAMP_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        ((RowType) EXPECTED_UNIX_TIMESTAMP_SCHEMA.toRowDataType()));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        UNIX_TIMESTAMP_TABLEID, EXPECTED_UNIX_TIMESTAMP_SCHEMA)));

        // In Berlin, "1970-01-01 08:00:01.001" formatted by "yyyy-MM-dd HH:mm:ss.SSS" ==> 25201L
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1970-01-01 08:00:01.001"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS")
                                }));
        DataChangeEvent insertEventExpect1 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), 25201L, 25201L}));
        transform.processElement(new StreamRecord<>(insertEvent1));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect1));

        // In Berlin, "1970-01-01 08:00:01.001 +0800" formatted by "yyyy-MM-dd HH:mm:ss.SSS X" ==>
        // 1L
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("1970-01-01 08:00:01.001 +0800"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS X")
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {new BinaryStringData("2"), 25201L, 1L}));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));

        // In Berlin, "1970-01-01 08:00:01.001 +0800" formatted by "yyyy-MM-dd HH:mm:ss.SSS" ==>
        // 25201L
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new BinaryStringData("1970-01-01 08:00:01.001 +0800"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS")
                                }));
        DataChangeEvent insertEventExpect3 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {new BinaryStringData("3"), 25201L, 25201L}));
        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect3));

        // In Berlin, "1970-01-01 08:00:01.001" formatted by "yyyy-MM-dd HH:mm:ss.SSS X" ==>
        // -9223372036854775808L
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new BinaryStringData("1970-01-01 08:00:01.001"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS X")
                                }));
        DataChangeEvent insertEventExpect4 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"), 25201L, -9223372036854775808L
                                }));
        transform.processElement(new StreamRecord<>(insertEvent4));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect4));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testUnixTimestampTransformInShanghai() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                UNIX_TIMESTAMP_TABLEID.identifier(),
                                "col1,"
                                        + " UNIX_TIMESTAMP(date_time_str) as unix_timestamp,"
                                        + " UNIX_TIMESTAMP(date_time_str, unix_timestamp_format) as unix_timestamp_format",
                                null)
                        .addTimezone("Asia/Shanghai")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(UNIX_TIMESTAMP_TABLEID, UNIX_TIMESTAMP_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) UNIX_TIMESTAMP_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        ((RowType) EXPECTED_UNIX_TIMESTAMP_SCHEMA.toRowDataType()));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        UNIX_TIMESTAMP_TABLEID, EXPECTED_UNIX_TIMESTAMP_SCHEMA)));

        // In Shanghai, "1970-01-01 08:00:01.001" formatted by "yyyy-MM-dd HH:mm:ss.SSS" ==> 1L
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1970-01-01 08:00:01.001"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS")
                                }));
        DataChangeEvent insertEventExpect1 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), 1L, 1L}));
        transform.processElement(new StreamRecord<>(insertEvent1));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect1));

        // In Shanghai, "1970-01-01 08:00:01.001 +0100" formatted by "yyyy-MM-dd HH:mm:ss.SSS X" ==>
        // 1L
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("1970-01-01 08:00:01.001 +0100"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS X")
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {new BinaryStringData("2"), 1L, 25201L}));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));

        // In Shanghai, "1970-01-01 08:00:01.001 +0100" formatted by "yyyy-MM-dd HH:mm:ss.SSS" ==>
        // 1L
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new BinaryStringData("1970-01-01 08:00:01.001 +0100"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS")
                                }));
        DataChangeEvent insertEventExpect3 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {new BinaryStringData("3"), 1L, 1L}));
        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect3));

        // In Shanghai, "1970-01-01 08:00:01.001" formatted by "yyyy-MM-dd HH:mm:ss.SSS X" ==>
        // -9223372036854775808L
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new BinaryStringData("1970-01-01 08:00:01.001"),
                                    new BinaryStringData("yyyy-MM-dd HH:mm:ss.SSS X")
                                }));
        DataChangeEvent insertEventExpect4 =
                DataChangeEvent.insertEvent(
                        UNIX_TIMESTAMP_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"), 1L, -9223372036854775808L
                                }));
        transform.processElement(new StreamRecord<>(insertEvent4));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect4));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testTimestampdiffTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMPDIFF_TABLEID.identifier(),
                                "col1, TIMESTAMPDIFF(SECOND, LOCALTIMESTAMP, CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) as second_diff,"
                                        + " TIMESTAMPDIFF(MINUTE, LOCALTIMESTAMP, CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) as minute_diff,"
                                        + " TIMESTAMPDIFF(HOUR, LOCALTIMESTAMP, CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) as hour_diff,"
                                        + " TIMESTAMPDIFF(DAY, LOCALTIMESTAMP, CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) as day_diff,"
                                        + " TIMESTAMPDIFF(MONTH, LOCALTIMESTAMP, CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) as month_diff,"
                                        + " TIMESTAMPDIFF(YEAR, LOCALTIMESTAMP, CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) as year_diff",
                                "col1='1'")
                        .addTransform(
                                TIMESTAMPDIFF_TABLEID.identifier(),
                                "col1, TIMESTAMPDIFF(SECOND, LOCALTIMESTAMP, CAST(NOW() AS TIMESTAMP)) as second_diff,"
                                        + " TIMESTAMPDIFF(MINUTE, LOCALTIMESTAMP, CAST(NOW() AS TIMESTAMP)) as minute_diff,"
                                        + " TIMESTAMPDIFF(HOUR, LOCALTIMESTAMP, CAST(NOW() AS TIMESTAMP)) as hour_diff,"
                                        + " TIMESTAMPDIFF(DAY, LOCALTIMESTAMP, CAST(NOW() AS TIMESTAMP)) as day_diff,"
                                        + " TIMESTAMPDIFF(MONTH, LOCALTIMESTAMP, CAST(NOW() AS TIMESTAMP)) as month_diff,"
                                        + " TIMESTAMPDIFF(YEAR, LOCALTIMESTAMP, CAST(NOW() AS TIMESTAMP)) as year_diff",
                                "col1='2'")
                        .addTimezone("Asia/Shanghai")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMPDIFF_TABLEID, TIMESTAMPDIFF_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMESTAMPDIFF_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), null, null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), 0, 0, 0, 0, 0, 0}));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(TIMESTAMPDIFF_TABLEID, TIMESTAMPDIFF_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"), null, null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("2"), 0, 0, 0, 0, 0, 0}));

        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));
    }

    @Test
    void testTimestampdiffTransformData() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMPDIFF_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit,"
                                        + " TIMESTAMPDIFF(SECOND, start_timestamp, end_timestamp) as timestamp_timestamp,"
                                        + " TIMESTAMPDIFF(SECOND, start_timestamp, end_timestamp_ltz) as timestamp_timestamp_ltz,"
                                        + " TIMESTAMPDIFF(SECOND, start_timestamp_ltz, end_timestamp) as timestamp_ltz_timestamp,"
                                        + " TIMESTAMPDIFF(SECOND, start_timestamp_ltz, end_timestamp_ltz) as timestamp_ltz_timestamp_ltz",
                                "time_interval_unit='SECOND'")
                        .addTransform(
                                TIMESTAMPDIFF_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit,"
                                        + " TIMESTAMPDIFF(MINUTE, start_timestamp, end_timestamp) as timestamp_timestamp,"
                                        + " TIMESTAMPDIFF(MINUTE, start_timestamp, end_timestamp_ltz) as timestamp_timestamp_ltz,"
                                        + " TIMESTAMPDIFF(MINUTE, start_timestamp_ltz, end_timestamp) as timestamp_ltz_timestamp,"
                                        + " TIMESTAMPDIFF(MINUTE, start_timestamp_ltz, end_timestamp_ltz) as timestamp_ltz_timestamp_ltz",
                                "time_interval_unit='MINUTE'")
                        .addTransform(
                                TIMESTAMPDIFF_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit,"
                                        + " TIMESTAMPDIFF(HOUR, start_timestamp, end_timestamp) as timestamp_timestamp,"
                                        + " TIMESTAMPDIFF(HOUR, start_timestamp, end_timestamp_ltz) as timestamp_timestamp_ltz,"
                                        + " TIMESTAMPDIFF(HOUR, start_timestamp_ltz, end_timestamp) as timestamp_ltz_timestamp,"
                                        + " TIMESTAMPDIFF(HOUR, start_timestamp_ltz, end_timestamp_ltz) as timestamp_ltz_timestamp_ltz",
                                "time_interval_unit='HOUR'")
                        .addTransform(
                                TIMESTAMPDIFF_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit,"
                                        + " TIMESTAMPDIFF(DAY, start_timestamp, end_timestamp) as timestamp_timestamp,"
                                        + " TIMESTAMPDIFF(DAY, start_timestamp, end_timestamp_ltz) as timestamp_timestamp_ltz,"
                                        + " TIMESTAMPDIFF(DAY, start_timestamp_ltz, end_timestamp) as timestamp_ltz_timestamp,"
                                        + " TIMESTAMPDIFF(DAY, start_timestamp_ltz, end_timestamp_ltz) as timestamp_ltz_timestamp_ltz",
                                "time_interval_unit='DAY'")
                        .addTransform(
                                TIMESTAMPDIFF_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit,"
                                        + " TIMESTAMPDIFF(MONTH, start_timestamp, end_timestamp) as timestamp_timestamp,"
                                        + " TIMESTAMPDIFF(MONTH, start_timestamp, end_timestamp_ltz) as timestamp_timestamp_ltz,"
                                        + " TIMESTAMPDIFF(MONTH, start_timestamp_ltz, end_timestamp) as timestamp_ltz_timestamp,"
                                        + " TIMESTAMPDIFF(MONTH, start_timestamp_ltz, end_timestamp_ltz) as timestamp_ltz_timestamp_ltz",
                                "time_interval_unit='MONTH'")
                        .addTransform(
                                TIMESTAMPDIFF_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit,"
                                        + " TIMESTAMPDIFF(YEAR, start_timestamp, end_timestamp) as timestamp_timestamp,"
                                        + " TIMESTAMPDIFF(YEAR, start_timestamp, end_timestamp_ltz) as timestamp_timestamp_ltz,"
                                        + " TIMESTAMPDIFF(YEAR, start_timestamp_ltz, end_timestamp) as timestamp_ltz_timestamp,"
                                        + " TIMESTAMPDIFF(YEAR, start_timestamp_ltz, end_timestamp_ltz) as timestamp_ltz_timestamp_ltz",
                                "time_interval_unit='YEAR'")
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMPDIFF_DATA_TABLEID, TIMESTAMPDIFF_DATA_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        TIMESTAMPDIFF_DATA_TABLEID,
                                        EXPECTED_TIMESTAMPDIFF_DATA_SCHEMA)));

        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        ((RowType) TIMESTAMPDIFF_DATA_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator recordDataGeneratorExpect =
                new BinaryRecordDataGenerator(
                        ((RowType) EXPECTED_TIMESTAMPDIFF_DATA_SCHEMA.toRowDataType()));
        // 1970-01-01 00:00:00 ~ 2025-01-01 00:00:00, Second: 1735689600
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("SECOND"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0)
                                }));
        DataChangeEvent insertEventExpect1 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("SECOND"),
                                    1735689600,
                                    1735689600,
                                    1735689600,
                                    1735689600
                                }));
        transform.processElement(new StreamRecord<>(insertEvent1));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect1));
        // 1970-01-01 00:00:00 ~ 2025-01-01 00:00:00, Minute: 28928160
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("MINUTE"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0)
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("MINUTE"),
                                    28928160,
                                    28928160,
                                    28928160,
                                    28928160
                                }));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));
        // 1970-01-01 00:00:00 ~ 2025-01-01 00:00:00, Hour: 482136
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new BinaryStringData("HOUR"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0)
                                }));
        DataChangeEvent insertEventExpect3 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new BinaryStringData("HOUR"),
                                    482136,
                                    482136,
                                    482136,
                                    482136
                                }));
        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect3));
        // 1970-01-01 00:00:00 ~ 2025-01-01 00:00:00, Day: 20089
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new BinaryStringData("DAY"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0)
                                }));
        DataChangeEvent insertEventExpect4 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new BinaryStringData("DAY"),
                                    20089,
                                    20089,
                                    20089,
                                    20089
                                }));
        transform.processElement(new StreamRecord<>(insertEvent4));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect4));
        // 1970-01-01 00:00:00 ~ 2025-01-01 00:00:00, Month: 660
        DataChangeEvent insertEvent5 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("5"),
                                    new BinaryStringData("MONTH"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0)
                                }));
        DataChangeEvent insertEventExpect5 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("5"),
                                    new BinaryStringData("MONTH"),
                                    660,
                                    660,
                                    660,
                                    660
                                }));
        transform.processElement(new StreamRecord<>(insertEvent5));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect5));
        // 1970-01-01 00:00:00 ~ 2025-01-01 00:00:00, Year: 660
        DataChangeEvent insertEvent6 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("6"),
                                    new BinaryStringData("YEAR"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0)
                                }));
        DataChangeEvent insertEventExpect6 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("6"),
                                    new BinaryStringData("YEAR"),
                                    55,
                                    55,
                                    55,
                                    55
                                }));

        transform.processElement(new StreamRecord<>(insertEvent6));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect6));
        // 1970-01-01 00:00:00 ~ 9999-12-31 23:59:59, Year: 8029
        DataChangeEvent insertEvent7 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("7"),
                                    new BinaryStringData("YEAR"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(253402271999000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(253402271999000L, 0)
                                }));
        DataChangeEvent insertEventExpect7 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("7"),
                                    new BinaryStringData("YEAR"),
                                    8029,
                                    8029,
                                    8029,
                                    8029
                                }));
        transform.processElement(new StreamRecord<>(insertEvent7));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect7));
        // 1970-01-01 00:00:00 ~ 9999-12-31 23:59:59, Second: null ( > Integer.MAX_VALUE)
        DataChangeEvent insertEvent8 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("8"),
                                    new BinaryStringData("SECOND"),
                                    TimestampData.fromMillis(0, 0),
                                    TimestampData.fromMillis(253402271999000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(253402271999000L, 0)
                                }));
        DataChangeEvent insertEventExpect8 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("8"),
                                    new BinaryStringData("SECOND"),
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent8));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect8));
        // 1970-01-01 00:00:00 ~ null, Year: null
        DataChangeEvent insertEvent9 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("9"),
                                    new BinaryStringData("YEAR"),
                                    TimestampData.fromMillis(0, 0),
                                    null,
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                    null
                                }));
        DataChangeEvent insertEventExpect9 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPDIFF_DATA_TABLEID,
                        recordDataGeneratorExpect.generate(
                                new Object[] {
                                    new BinaryStringData("9"),
                                    new BinaryStringData("YEAR"),
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent9));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect9));
    }

    @Test
    void testTimestampAddTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMPADD_TABLEID.identifier(),
                                "col1, DATE_FORMAT(TIMESTAMPADD(SECOND, 1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as second_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(MINUTE, 1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as minute_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(HOUR, 1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as hour_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(DAY, 1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as day_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(MONTH, 1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as month_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(YEAR, 1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as year_add",
                                "col1='1'")
                        .addTransform(
                                TIMESTAMPADD_TABLEID.identifier(),
                                "col1, DATE_FORMAT(TIMESTAMPADD(SECOND, -1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as second_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(MINUTE, -1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as minute_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(HOUR, -1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as hour_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(DAY, -1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as day_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(MONTH, -1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as month_add,"
                                        + " DATE_FORMAT(TIMESTAMPADD(YEAR, -1, TO_TIMESTAMP('2024-10-01 00:00:00')), 'yyyy-MM-dd HH:mm:ss') as year_add",
                                "col1='2'")
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMPADD_TABLEID, TIMESTAMPADD_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMESTAMPADD_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), null, null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2024-10-01 00:00:01"),
                                    new BinaryStringData("2024-10-01 00:01:00"),
                                    new BinaryStringData("2024-10-01 01:00:00"),
                                    new BinaryStringData("2024-10-02 00:00:00"),
                                    new BinaryStringData("2024-11-01 00:00:00"),
                                    new BinaryStringData("2025-10-01 00:00:00")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(TIMESTAMPADD_TABLEID, TIMESTAMPADD_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"), null, null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("2024-09-30 23:59:59"),
                                    new BinaryStringData("2024-09-30 23:59:00"),
                                    new BinaryStringData("2024-09-30 23:00:00"),
                                    new BinaryStringData("2024-09-30 00:00:00"),
                                    new BinaryStringData("2024-09-01 00:00:00"),
                                    new BinaryStringData("2023-10-01 00:00:00")
                                }));

        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testTimestampaddTransformData() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMESTAMPADD_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit, interval_value,"
                                        + " TIMESTAMPADD(SECOND, interval_value, time_point_timestamp) as time_point_timestamp,"
                                        + " TIMESTAMPADD(SECOND, interval_value, time_point_timestamp_ltz) as time_point_timestamp_ltz",
                                "time_interval_unit='SECOND'")
                        .addTransform(
                                TIMESTAMPADD_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit, interval_value,"
                                        + " TIMESTAMPADD(MINUTE, interval_value, time_point_timestamp) as time_point_timestamp,"
                                        + " TIMESTAMPADD(MINUTE, interval_value, time_point_timestamp_ltz) as time_point_timestamp_ltz",
                                "time_interval_unit='MINUTE'")
                        .addTransform(
                                TIMESTAMPADD_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit, interval_value,"
                                        + " TIMESTAMPADD(HOUR, interval_value, time_point_timestamp) as time_point_timestamp,"
                                        + " TIMESTAMPADD(HOUR, interval_value, time_point_timestamp_ltz) as time_point_timestamp_ltz",
                                "time_interval_unit='HOUR'")
                        .addTransform(
                                TIMESTAMPADD_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit, interval_value,"
                                        + " TIMESTAMPADD(DAY, interval_value, time_point_timestamp) as time_point_timestamp,"
                                        + " TIMESTAMPADD(DAY, interval_value, time_point_timestamp_ltz) as time_point_timestamp_ltz",
                                "time_interval_unit='DAY'")
                        .addTransform(
                                TIMESTAMPADD_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit, interval_value,"
                                        + " TIMESTAMPADD(MONTH, interval_value, time_point_timestamp) as time_point_timestamp,"
                                        + " TIMESTAMPADD(MONTH, interval_value, time_point_timestamp_ltz) as time_point_timestamp_ltz",
                                "time_interval_unit='MONTH'")
                        .addTransform(
                                TIMESTAMPADD_DATA_TABLEID.identifier(),
                                "col1, time_interval_unit, interval_value,"
                                        + " TIMESTAMPADD(YEAR, interval_value, time_point_timestamp) as time_point_timestamp,"
                                        + " TIMESTAMPADD(YEAR, interval_value, time_point_timestamp_ltz) as time_point_timestamp_ltz",
                                "time_interval_unit='YEAR'")
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(TIMESTAMPADD_DATA_TABLEID, TIMESTAMPADD_DATA_SCHEMA);
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        TIMESTAMPADD_DATA_TABLEID, TIMESTAMPADD_DATA_SCHEMA)));
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMESTAMPADD_DATA_SCHEMA.toRowDataType()));

        // 1970-01-01 00:00:00 + Second: 1735689600 = 2025-01-01 00:00:00
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("SECOND"),
                                    1735689600,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect1 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("SECOND"),
                                    1735689600,
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0),
                                }));
        transform.processElement(new StreamRecord<>(insertEvent1));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect1));

        // 1970-01-01 00:00:00 + Minute: 28928160 = 2025-01-01 00:00:00
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("MINUTE"),
                                    28928160,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new BinaryStringData("MINUTE"),
                                    28928160,
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0),
                                }));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));

        // 1970-01-01 00:00:00 + Hour: 482136 = 2025-01-01 00:00:00
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new BinaryStringData("HOUR"),
                                    482136,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect3 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new BinaryStringData("HOUR"),
                                    482136,
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0),
                                }));
        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect3));

        // 1970-01-01 00:00:00 + Day: 20089 = 2025-01-01 00:00:00
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new BinaryStringData("DAY"),
                                    20089,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect4 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new BinaryStringData("DAY"),
                                    20089,
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0),
                                }));
        transform.processElement(new StreamRecord<>(insertEvent4));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect4));

        // 1970-01-01 00:00:00 + Month: 660 = 2025-01-01 00:00:00
        DataChangeEvent insertEvent5 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("5"),
                                    new BinaryStringData("MONTH"),
                                    660,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect5 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("5"),
                                    new BinaryStringData("MONTH"),
                                    660,
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0),
                                }));
        transform.processElement(new StreamRecord<>(insertEvent5));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect5));

        // 1970-01-01 00:00:00 + Year: 55 = 2025-01-01 00:00:00
        DataChangeEvent insertEvent6 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("6"),
                                    new BinaryStringData("YEAR"),
                                    55,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect6 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("6"),
                                    new BinaryStringData("YEAR"),
                                    55,
                                    TimestampData.fromMillis(1735689600000L, 0),
                                    LocalZonedTimestampData.fromEpochMillis(1735689600000L, 0),
                                }));
        transform.processElement(new StreamRecord<>(insertEvent6));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect6));

        // 1970-01-01 00:00:00 + Year: null = null
        DataChangeEvent insertEvent7 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("7"),
                                    new BinaryStringData("YEAR"),
                                    null,
                                    TimestampData.fromMillis(0, 0),
                                    LocalZonedTimestampData.fromEpochMillis(0, 0),
                                }));
        DataChangeEvent insertEventExpect7 =
                DataChangeEvent.insertEvent(
                        TIMESTAMPADD_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("7"),
                                    new BinaryStringData("YEAR"),
                                    null,
                                    null,
                                    null,
                                }));
        transform.processElement(new StreamRecord<>(insertEvent7));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect7));
    }

    @Test
    void testTimezoneTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                TIMEZONE_TABLEID.identifier(),
                                "col1, DATE_FORMAT(TO_TIMESTAMP('2024-08-01 00:00:00'), 'yyyy-MM-dd HH:mm:ss') as datetime_value",
                                null)
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(TIMEZONE_TABLEID, TIMEZONE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) TIMEZONE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        TIMEZONE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        TIMEZONE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("2024-08-01 00:00:00")
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(TIMEZONE_TABLEID, TIMEZONE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    void testNullCastTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                NULL_TABLEID.identifier(),
                                "col1"
                                        + ",colString"
                                        + ",cast(colString as int) as nullInt"
                                        + ",cast(colString as boolean) as nullBoolean"
                                        + ",cast(colString as tinyint) as nullTinyint"
                                        + ",cast(colString as smallint) as nullSmallint"
                                        + ",cast(colString as bigint) as nullBigint"
                                        + ",cast(colString as float) as nullFloat"
                                        + ",cast(colString as double) as nullDouble"
                                        + ",cast(colString as char) as nullChar"
                                        + ",cast(colString as varchar) as nullVarchar"
                                        + ",cast(colString as DECIMAL(4,2)) as nullDecimal"
                                        + ",cast(colString as TIMESTAMP(3)) as nullTimestamp",
                                null)
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(NULL_TABLEID, NULL_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) NULL_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        NULL_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(NULL_TABLEID, NULL_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEvent));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testCastTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(col1 as int) as castInt"
                                        + ",cast(col1 as boolean) as castBoolean"
                                        + ",cast(col1 as tinyint) as castTinyint"
                                        + ",cast(col1 as smallint) as castSmallint"
                                        + ",cast(col1 as bigint) as castBigint"
                                        + ",cast(col1 as float) as castFloat"
                                        + ",cast(col1 as double) as castDouble"
                                        + ",cast(col1 as char) as castChar"
                                        + ",cast(col1 as varchar) as castVarchar"
                                        + ",cast(col1 as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '1'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(castInt as int) as castInt"
                                        + ",cast(castInt as boolean) as castBoolean"
                                        + ",cast(castInt as tinyint) as castTinyint"
                                        + ",cast(castInt as smallint) as castSmallint"
                                        + ",cast(castInt as bigint) as castBigint"
                                        + ",cast(castInt as float) as castFloat"
                                        + ",cast(castInt as double) as castDouble"
                                        + ",cast(castInt as char) as castChar"
                                        + ",cast(castInt as varchar) as castVarchar"
                                        + ",cast(castInt as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '2'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(castBoolean as int) as castInt"
                                        + ",cast(castBoolean as boolean) as castBoolean"
                                        + ",cast(castBoolean as tinyint) as castTinyint"
                                        + ",cast(castBoolean as smallint) as castSmallint"
                                        + ",cast(castBoolean as bigint) as castBigint"
                                        + ",castFloat"
                                        + ",castDouble"
                                        + ",cast(castBoolean as char) as castChar"
                                        + ",cast(castBoolean as varchar) as castVarchar"
                                        + ",castDecimal"
                                        + ", castTimestamp",
                                "col1 = '3'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(castTinyint as int) as castInt"
                                        + ",cast(castTinyint as boolean) as castBoolean"
                                        + ",cast(castTinyint as tinyint) as castTinyint"
                                        + ",cast(castTinyint as smallint) as castSmallint"
                                        + ",cast(castTinyint as bigint) as castBigint"
                                        + ",cast(castTinyint as float) as castFloat"
                                        + ",cast(castTinyint as double) as castDouble"
                                        + ",cast(castTinyint as char) as castChar"
                                        + ",cast(castTinyint as varchar) as castVarchar"
                                        + ",cast(castTinyint as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '4'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(castSmallint as int) as castInt"
                                        + ",cast(castSmallint as boolean) as castBoolean"
                                        + ",cast(castSmallint as tinyint) as castTinyint"
                                        + ",cast(castSmallint as smallint) as castSmallint"
                                        + ",cast(castSmallint as bigint) as castBigint"
                                        + ",cast(castSmallint as float) as castFloat"
                                        + ",cast(castSmallint as double) as castDouble"
                                        + ",cast(castSmallint as char) as castChar"
                                        + ",cast(castSmallint as varchar) as castVarchar"
                                        + ",cast(castSmallint as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '5'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(castBigint as int) as castInt"
                                        + ",cast(castBigint as boolean) as castBoolean"
                                        + ",cast(castBigint as tinyint) as castTinyint"
                                        + ",cast(castBigint as smallint) as castSmallint"
                                        + ",cast(castBigint as bigint) as castBigint"
                                        + ",cast(castBigint as float) as castFloat"
                                        + ",cast(castBigint as double) as castDouble"
                                        + ",cast(castBigint as char) as castChar"
                                        + ",cast(castBigint as varchar) as castVarchar"
                                        + ",cast(castBigint as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '6'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",castInt"
                                        + ",cast(castFloat as boolean) as castBoolean"
                                        + ",castTinyint"
                                        + ",castSmallint"
                                        + ",castBigint"
                                        + ",cast(castFloat as float) as castFloat"
                                        + ",cast(castFloat as double) as castDouble"
                                        + ",cast(castFloat as char) as castChar"
                                        + ",cast(castFloat as varchar) as castVarchar"
                                        + ",cast(castFloat as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '7'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",castInt"
                                        + ",cast(castDouble as boolean) as castBoolean"
                                        + ",castTinyint"
                                        + ",castSmallint"
                                        + ",castBigint"
                                        + ",cast(castDouble as float) as castFloat"
                                        + ",cast(castDouble as double) as castDouble"
                                        + ",cast(castDouble as char) as castChar"
                                        + ",cast(castDouble as varchar) as castVarchar"
                                        + ",cast(castDouble as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '8'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",castInt"
                                        + ",cast(castDecimal as boolean) as castBoolean"
                                        + ",castTinyint"
                                        + ",castSmallint"
                                        + ",castBigint"
                                        + ",cast(castDecimal as float) as castFloat"
                                        + ",cast(castDecimal as double) as castDouble"
                                        + ",cast(castDecimal as char) as castChar"
                                        + ",cast(castDecimal as varchar) as castVarchar"
                                        + ",cast(castDecimal as DECIMAL(4,2)) as castDecimal"
                                        + ", castTimestamp",
                                "col1 = '9'")
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",castInt"
                                        + ",castBoolean"
                                        + ",castTinyint"
                                        + ",castSmallint"
                                        + ",castBigint"
                                        + ",castFloat"
                                        + ",castDouble"
                                        + ",castChar"
                                        + ",cast(castTimestamp as varchar) as castVarchar"
                                        + ",castDecimal"
                                        + ",cast('1970-01-01T00:00:01.234' as TIMESTAMP(3)) as castTimestamp",
                                "col1 = '10'")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(CAST_TABLEID, CAST_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CAST_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect1 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new Integer(1),
                                    new Boolean(false),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Long(1),
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(CAST_TABLEID, CAST_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent1));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect1));
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new Integer(1),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("2"),
                                    new Integer(1),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Long(1),
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));
        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    null,
                                    new Boolean(true),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect3 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("3"),
                                    new Integer(1),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Long(1),
                                    null,
                                    null,
                                    new BinaryStringData("true"),
                                    new BinaryStringData("true"),
                                    null,
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect3));
        DataChangeEvent insertEvent4 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    null,
                                    null,
                                    new Byte("1"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect4 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("4"),
                                    new Integer(1),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Long(1),
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent4));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect4));
        DataChangeEvent insertEvent5 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("5"),
                                    null,
                                    null,
                                    null,
                                    new Short("1"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect5 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("5"),
                                    new Integer(1),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Long(1),
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent5));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect5));
        DataChangeEvent insertEvent6 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("6"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    new Long(1),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect6 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("6"),
                                    new Integer(1),
                                    new Boolean(true),
                                    new Byte("1"),
                                    new Short("1"),
                                    new Long(1),
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1"),
                                    new BinaryStringData("1"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent6));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect6));
        DataChangeEvent insertEvent7 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("7"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new Float(1.0f),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect7 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("7"),
                                    null,
                                    new Boolean(true),
                                    null,
                                    null,
                                    null,
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1.0"),
                                    new BinaryStringData("1.0"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent7));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect7));
        DataChangeEvent insertEvent8 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("8"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new Double(1.0d),
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect8 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("8"),
                                    null,
                                    new Boolean(true),
                                    null,
                                    null,
                                    null,
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1.0"),
                                    new BinaryStringData("1.0"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent8));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect8));
        DataChangeEvent insertEvent9 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("9"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        DataChangeEvent insertEventExpect9 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("9"),
                                    null,
                                    new Boolean(true),
                                    null,
                                    null,
                                    null,
                                    new Float(1.0f),
                                    new Double(1.0d),
                                    new BinaryStringData("1.00"),
                                    new BinaryStringData("1.00"),
                                    DecimalData.fromBigDecimal(new BigDecimal(1.0), 4, 2),
                                    null
                                }));
        transform.processElement(new StreamRecord<>(insertEvent9));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect9));

        DataChangeEvent insertEvent10 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("10"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    TimestampData.fromMillis(1234, 0)
                                }));
        DataChangeEvent insertEventExpect10 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("10"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new BinaryStringData("1970-01-01T00:00:01.234"),
                                    null,
                                    TimestampData.fromMillis(1234, 0)
                                }));
        transform.processElement(new StreamRecord<>(insertEvent10));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect10));
    }

    @Test
    void testCastErrorTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CAST_TABLEID.identifier(),
                                "col1"
                                        + ",cast(castFloat as int) as castInt"
                                        + ",cast(castFloat as boolean) as castBoolean"
                                        + ",cast(castFloat as tinyint) as castTinyint"
                                        + ",cast(castFloat as smallint) as castSmallint"
                                        + ",cast(castFloat as bigint) as castBigint"
                                        + ",cast(castFloat as float) as castFloat"
                                        + ",cast(castFloat as double) as castDouble"
                                        + ",cast(castFloat as char) as castChar"
                                        + ",cast(castFloat as varchar) as castVarchar"
                                        + ",cast(castFloat as DECIMAL(4,2)) as castDecimal"
                                        + ",cast(castFloat as TIMESTAMP(3)) as castTimestamp",
                                "col1 = '1'")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(CAST_TABLEID, CAST_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CAST_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        CAST_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new Float(1.0f),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(new CreateTableEvent(CAST_TABLEID, CAST_SCHEMA)));
        Assertions.assertThatThrownBy(
                        () -> transform.processElement(new StreamRecord<>(insertEvent1)))
                .isExactlyInstanceOf(TransformException.class)
                .hasMessageContaining(
                        "Failed to post-transform with\n"
                                + "\tDataChangeEvent{tableId=my_company.my_branch.data_cast, before=null, after={col1: STRING NOT NULL -> 1, castInt: INT -> null, castBoolean: BOOLEAN -> null, castTinyint: TINYINT -> null, castSmallint: SMALLINT -> null, castBigint: BIGINT -> null, castFloat: FLOAT -> 1.0, castDouble: DOUBLE -> null, castChar: STRING -> null, castVarchar: STRING -> null, castDecimal: DECIMAL(4, 2) -> null, castTimestamp: TIMESTAMP(3) -> null}, op=INSERT, meta=()}\n"
                                + "for table\n"
                                + "\tmy_company.my_branch.data_cast\n"
                                + "from schema\n"
                                + "\tcolumns={`col1` STRING NOT NULL,`castInt` INT,`castBoolean` BOOLEAN,`castTinyint` TINYINT,`castSmallint` SMALLINT,`castBigint` BIGINT,`castFloat` FLOAT,`castDouble` DOUBLE,`castChar` STRING,`castVarchar` STRING,`castDecimal` DECIMAL(4, 2),`castTimestamp` TIMESTAMP(3)}, primaryKeys=col1, options=()\n"
                                + "to schema\n"
                                + "\tcolumns={`col1` STRING NOT NULL,`castInt` INT,`castBoolean` BOOLEAN,`castTinyint` TINYINT,`castSmallint` SMALLINT,`castBigint` BIGINT,`castFloat` FLOAT,`castDouble` DOUBLE,`castChar` STRING,`castVarchar` STRING,`castDecimal` DECIMAL(4, 2),`castTimestamp` TIMESTAMP(3)}, primaryKeys=col1, options=().")
                .cause()
                .hasRootCauseInstanceOf(DateTimeParseException.class)
                .hasRootCauseMessage("Text '1.0' could not be parsed at index 0");
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testCompareTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                COMPARE_TABLEID.identifier(),
                                "col1, 2.1 > 1 as numerical_equal,"
                                        + " '2024-01-01 00:00:00' < '2024-08-01 00:00:00' as string_equal,"
                                        + " LOCALTIME <= CURRENT_TIME as time_equal,"
                                        + " TO_TIMESTAMP('2024-01-01 00:00:00') <= TO_TIMESTAMP('2024-08-01 00:00:00') as timestamp_equal,"
                                        + " TO_DATE(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd')) >= TO_DATE(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd')) as date_equal",
                                "2 > 1")
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);

        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(COMPARE_TABLEID, COMPARE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) COMPARE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        COMPARE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        COMPARE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), true, true, true, true, true
                                }));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(new CreateTableEvent(COMPARE_TABLEID, COMPARE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
    }

    @Test
    void testCompareErrorTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                COMPARE_TABLEID.identifier(),
                                "col1, 2.1 > TO_TIMESTAMP('2024-01-01 00:00:00') as numerical_equal,"
                                        + " '2024-01-01 00:00:00' < '2024-08-01 00:00:00' as string_equal,"
                                        + " LOCALTIME <= CURRENT_TIME as time_equal,"
                                        + " TO_TIMESTAMP('2024-01-01 00:00:00') <= TO_TIMESTAMP('2024-08-01 00:00:00') as timestamp_equal,"
                                        + " TO_DATE(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd')) >= TO_DATE(DATE_FORMAT(LOCALTIMESTAMP, 'yyyy-MM-dd')) as date_equal",
                                "2 > 1")
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(COMPARE_TABLEID, COMPARE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) COMPARE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        COMPARE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), null, null, null, null, null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        COMPARE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("1"), true, true, true, true, true
                                }));
        Assertions.assertThatThrownBy(
                        () -> {
                            transform.processElement(new StreamRecord<>(createTableEvent));
                        })
                .isExactlyInstanceOf(TransformException.class)
                .hasMessageContaining(
                        "Failed to post-transform with\n"
                                + "\tCreateTableEvent{tableId=my_company.my_branch.compare_table, schema=columns={`col1` STRING NOT NULL,`numerical_equal` BOOLEAN,`string_equal` BOOLEAN,`time_equal` BOOLEAN,`timestamp_equal` BOOLEAN,`date_equal` BOOLEAN}, primaryKeys=col1, options=()}\n"
                                + "for table\n"
                                + "\tmy_company.my_branch.compare_table\n"
                                + "from schema\n"
                                + "\t(Unknown)\n"
                                + "to schema\n"
                                + "\t(Unknown).")
                .cause()
                .isExactlyInstanceOf(CalciteContextException.class)
                .hasRootCauseInstanceOf(SqlValidatorException.class)
                .hasRootCauseMessage(
                        "Cannot apply '>' to arguments of type '<DECIMAL(2, 1)> > <TIMESTAMP(3)>'. Supported form(s): '<COMPARABLE_TYPE> > <COMPARABLE_TYPE>'");
    }

    @Test
    void testCompareDataTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                COMPARE_DATA_TABLEID.identifier(),
                                "id, c1 < 5 as float_equal, c2 > 2.5 as double_equal, c3 <= TO_TIMESTAMP('2024-01-01 00:00:00') as timestamp_equal",
                                null)
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(COMPARE_DATA_TABLEID, COMPARE_DATA_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) COMPARE_DATA_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        ((RowType) EXPECTD_COMPARE_DATA_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        COMPARE_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1,
                                    new Float(4f),
                                    new Double(3.5d),
                                    TimestampData.fromMillis(1672502400000L)
                                }));
        DataChangeEvent insertEventExpect1 =
                DataChangeEvent.insertEvent(
                        COMPARE_DATA_TABLEID,
                        expectRecordDataGenerator.generate(new Object[] {1, true, true, true}));

        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        COMPARE_DATA_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    2,
                                    new Float(10f),
                                    new Double(0d),
                                    TimestampData.fromMillis(1730390400000L)
                                }));
        DataChangeEvent insertEventExpect2 =
                DataChangeEvent.insertEvent(
                        COMPARE_DATA_TABLEID,
                        expectRecordDataGenerator.generate(new Object[] {2, false, false, false}));

        DataChangeEvent insertEvent3 =
                DataChangeEvent.insertEvent(
                        COMPARE_DATA_TABLEID,
                        recordDataGenerator.generate(new Object[] {3, null, null, null}));
        DataChangeEvent insertEventExpect3 =
                DataChangeEvent.insertEvent(
                        COMPARE_DATA_TABLEID,
                        expectRecordDataGenerator.generate(new Object[] {3, false, false, false}));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        COMPARE_DATA_TABLEID, EXPECTD_COMPARE_DATA_SCHEMA)));

        transform.processElement(new StreamRecord<>(insertEvent1));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect1));

        transform.processElement(new StreamRecord<>(insertEvent2));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect2));

        transform.processElement(new StreamRecord<>(insertEvent3));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect3));
    }

    @Test
    void testBuildInFunctionTransform() throws Exception {
        testExpressionConditionTransform(
                "TO_TIMESTAMP('1970-01-01 00:00:00') = TO_TIMESTAMP('1970-01-01', 'yyyy-MM-dd')");
        testExpressionConditionTransform(
                "TIMESTAMPDIFF(DAY, TO_TIMESTAMP('1970-01-01 00:00:00'), TO_TIMESTAMP('1970-01-02 00:00:00')) = 1");
        testExpressionConditionTransform("2 between 1 and 3");
        testExpressionConditionTransform("4 not between 1 and 3");
        testExpressionConditionTransform("2 in (1, 2, 3)");
        testExpressionConditionTransform("4 not in (1, 2, 3)");
        testExpressionConditionTransform("CHAR_LENGTH('abc') = 3");
        testExpressionConditionTransform("trim(' abc ') = 'abc'");
        testExpressionConditionTransform("REGEXP_REPLACE('123abc', '[a-zA-Z]', '') = '123'");
        testExpressionConditionTransform("concat('123', 'abc') = '123abc'");
        testExpressionConditionTransform("upper('abc') = 'ABC'");
        testExpressionConditionTransform("lower('ABC') = 'abc'");
        testExpressionConditionTransform("SUBSTR('ABC', -1) = 'C'");
        testExpressionConditionTransform("SUBSTR('ABC', -2, 2) = 'BC'");
        testExpressionConditionTransform("SUBSTR('ABC', 0) = 'ABC'");
        testExpressionConditionTransform("SUBSTR('ABC', 1) = 'ABC'");
        testExpressionConditionTransform("SUBSTR('ABC', 2, 2) = 'BC'");
        testExpressionConditionTransform("SUBSTR('ABC', 2, 100) = 'BC'");
        testExpressionConditionTransform("SUBSTRING('ABC', -1) = 'C'");
        testExpressionConditionTransform("SUBSTRING('ABC', -2, 2) = 'BC'");
        testExpressionConditionTransform("SUBSTRING('ABC', 0) = 'ABC'");
        testExpressionConditionTransform("SUBSTRING('ABC', 1) = 'ABC'");
        testExpressionConditionTransform("SUBSTRING('ABC', 2, 2) = 'BC'");
        testExpressionConditionTransform("SUBSTRING('ABC', 2, 100) = 'BC'");
        testExpressionConditionTransform("SUBSTRING('ABC' FROM -1) = 'C'");
        testExpressionConditionTransform("SUBSTRING('ABC' FROM -2 FOR 2) = 'BC'");
        testExpressionConditionTransform("SUBSTRING('ABC' FROM 0) = 'ABC'");
        testExpressionConditionTransform("SUBSTRING('ABC' FROM 1) = 'ABC'");
        testExpressionConditionTransform("SUBSTRING('ABC' FROM 2 FOR 2) = 'BC'");
        testExpressionConditionTransform("SUBSTRING('ABC' FROM 2 FOR 100) = 'BC'");
        testExpressionConditionTransform("'ABC' like '^[a-zA-Z]'");
        testExpressionConditionTransform("'123' not like '^[a-zA-Z]'");
        testExpressionConditionTransform("abs(2) = 2");
        testExpressionConditionTransform("ceil(2.4) = 3.0");
        testExpressionConditionTransform("ceiling(2.4) = 3.0");
        testExpressionConditionTransform("floor(2.5) = 2.0");
        testExpressionConditionTransform("round(3.1415926, 2) = 3.14");
        testExpressionConditionTransform("IF(2>0,1,0) = 1");
        testExpressionConditionTransform("COALESCE(null,1,2) = 1");
        testExpressionConditionTransform("1 + 1 = 2");
        testExpressionConditionTransform("1 - 1 = 0");
        testExpressionConditionTransform("1 * 1 = 1");
        testExpressionConditionTransform("3 % 2 = 1");
        testExpressionConditionTransform("1 < 2");
        testExpressionConditionTransform("1 <= 1");
        testExpressionConditionTransform("1 > 0");
        testExpressionConditionTransform("1 >= 1");
        testExpressionConditionTransform(
                "case 1 when 1 then 'a' when 2 then 'b' else 'c' end = 'a'");
        testExpressionConditionTransform("case col1 when '1' then true else false end");
        testExpressionConditionTransform("case when col1 = '1' then true else false end");
        testExpressionConditionTransform("cast(col1 as int) = 1");
        testExpressionConditionTransform("cast('true' as boolean)");
        testExpressionConditionTransform("cast(col1 as tinyint) = cast(1 as tinyint)");
        testExpressionConditionTransform("cast(col1 as smallint) = cast(1 as smallint)");
        testExpressionConditionTransform("cast(col1 as bigint) = cast(1 as bigint)");
        testExpressionConditionTransform("cast(col1 as float) = cast(1 as float)");
        testExpressionConditionTransform("cast(col1 as double) = cast(1 as double)");
        testExpressionConditionTransform("cast('1' as char) = '1'");
        testExpressionConditionTransform("cast(col1 as varchar) = '1'");
        testExpressionConditionTransform("cast(col1 as DECIMAL(4,2)) = cast(1.0 as DECIMAL(4,2))");
        testExpressionConditionTransform("cast(null as int) is null");
        testExpressionConditionTransform("cast(null as boolean) is null");
        testExpressionConditionTransform("cast(null as tinyint) is null");
        testExpressionConditionTransform("cast(null as smallint) is null");
        testExpressionConditionTransform("cast(null as bigint) is null");
        testExpressionConditionTransform("cast(null as float) is null");
        testExpressionConditionTransform("cast(null as double) is null");
        testExpressionConditionTransform("cast(null as char) is null");
        testExpressionConditionTransform("cast(null as varchar) is null");
        testExpressionConditionTransform("cast(null as DECIMAL(4,2)) is null");
        testExpressionConditionTransform("cast(null as TIMESTAMP(3)) is null");
    }

    private void testExpressionConditionTransform(String expression) throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                CONDITION_TABLEID.identifier(),
                                "col1, IF(" + expression + ", true, false) as condition_result",
                                expression)
                        .addTimezone("UTC")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(CONDITION_TABLEID, CONDITION_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) CONDITION_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        CONDITION_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), null}));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        CONDITION_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {new BinaryStringData("1"), true}));
        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(CONDITION_TABLEID, CONDITION_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testReduceSchemaTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                REDUCE_TABLEID.identifier(),
                                "id, upper(id) as uid, age + 1 as newage, lower(ref1) as ref1, 17 as seventeen",
                                "newage > 17 and ref2 > 17")
                        .addTimezone("GMT")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(REDUCE_TABLEID, REDUCE_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) REDUCE_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_REDUCE_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        REDUCE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Reference"),
                                    42
                                }));

        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        REDUCE_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    new BinaryStringData("ID001"),
                                    18,
                                    new BinaryStringData("reference"),
                                    17
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        REDUCE_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Reference"),
                                    42
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("UpdatedReference"),
                                    41
                                }));

        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        REDUCE_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    new BinaryStringData("ID001"),
                                    18,
                                    new BinaryStringData("reference"),
                                    17
                                }),
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    new BinaryStringData("ID001"),
                                    19,
                                    new BinaryStringData("updatedreference"),
                                    17
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(REDUCE_TABLEID, EXPECTED_REDUCE_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testWildcardSchemaTransform() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                WILDCARD_TABLEID.identifier(),
                                "*, age + 1 as newage",
                                "newage > 17")
                        .addTimezone("GMT")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent = new CreateTableEvent(WILDCARD_TABLEID, WILDCARD_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) WILDCARD_SCHEMA.toRowDataType()));
        BinaryRecordDataGenerator expectedRecordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) EXPECTED_WILDCARD_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        WILDCARD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                }));

        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        WILDCARD_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                    18
                                }));

        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        WILDCARD_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("Arisu"),
                                }));

        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        WILDCARD_TABLEID,
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    17,
                                    new BinaryStringData("Alice"),
                                    18
                                }),
                        expectedRecordDataGenerator.generate(
                                new Object[] {
                                    new BinaryStringData("id001"),
                                    18,
                                    new BinaryStringData("Arisu"),
                                    19
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(WILDCARD_TABLEID, EXPECTED_WILDCARD_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));

        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));

        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
        transformFunctionEventEventOperatorTestHarness.close();
    }

    @Test
    void testColumnNameMapping() throws Exception {
        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                COL_NAME_MAPPING_TABLEID.identifier(),
                                "*, class, foo-bar AS f0, bar-foo AS f1, `foo-bar`-`bar-foo` AS f2",
                                "`foo-bar`-`bar-foo` <> 0")
                        .build();
        RegularEventOperatorTestHarness<PostTransformOperator, Event>
                transformFunctionEventEventOperatorTestHarness =
                        RegularEventOperatorTestHarness.with(transform, 1);
        // Initialization
        transformFunctionEventEventOperatorTestHarness.open();
        // Create table
        CreateTableEvent createTableEvent =
                new CreateTableEvent(COL_NAME_MAPPING_TABLEID, COL_NAME_MAPPING_SCHEMA);
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(((RowType) COL_NAME_MAPPING_SCHEMA.toRowDataType()));
        // Insert
        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        COL_NAME_MAPPING_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1,
                                    2,
                                    3,
                                    4,
                                    BinaryStringData.fromString("class0"),
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent insertEventExpect =
                DataChangeEvent.insertEvent(
                        COL_NAME_MAPPING_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1, 2, 3, 4, BinaryStringData.fromString("class0"), -1, 1, -1
                                }));
        // Update
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        COL_NAME_MAPPING_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1,
                                    2,
                                    3,
                                    4,
                                    BinaryStringData.fromString("class0"),
                                    null,
                                    null,
                                    null
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    2,
                                    4,
                                    6,
                                    8,
                                    BinaryStringData.fromString("class1"),
                                    null,
                                    null,
                                    null
                                }));
        DataChangeEvent updateEventExpect =
                DataChangeEvent.updateEvent(
                        COL_NAME_MAPPING_TABLEID,
                        recordDataGenerator.generate(
                                new Object[] {
                                    1, 2, 3, 4, BinaryStringData.fromString("class0"), -1, 1, -1
                                }),
                        recordDataGenerator.generate(
                                new Object[] {
                                    2, 4, 6, 8, BinaryStringData.fromString("class1"), -2, 2, -2
                                }));

        transform.processElement(new StreamRecord<>(createTableEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(
                        new StreamRecord<>(
                                new CreateTableEvent(
                                        COL_NAME_MAPPING_TABLEID, COL_NAME_MAPPING_SCHEMA)));
        transform.processElement(new StreamRecord<>(insertEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(insertEventExpect));
        transform.processElement(new StreamRecord<>(updateEvent));
        Assertions.assertThat(
                        transformFunctionEventEventOperatorTestHarness.getOutputRecords().poll())
                .isEqualTo(new StreamRecord<>(updateEventExpect));
    }
}
