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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.common.types.DataTypes.DECIMAL;
import static org.apache.flink.cdc.common.types.DataTypes.VARCHAR;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.coerceObject;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.coerceRow;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.getLeastCommonSchema;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.getLeastCommonType;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.getSchemaDifference;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.isDataTypeCompatible;
import static org.apache.flink.cdc.common.utils.SchemaMergingUtils.isSchemaCompatible;

/** A test for the {@link SchemaMergingUtils}. */
class SchemaMergingUtilsTest {

    private static final TableId TABLE_ID = TableId.tableId("foo", "bar", "baz");

    private static final DataType CHAR = DataTypes.CHAR(17);
    private static final DataType VARCHAR = DataTypes.VARCHAR(17);
    private static final DataType STRING = DataTypes.STRING();

    private static final DataType BOOLEAN = DataTypes.BOOLEAN();
    private static final DataType BINARY = DataTypes.BINARY(17);
    private static final DataType VARBINARY = DataTypes.VARBINARY(17);
    private static final DataType SMALLINT = DataTypes.SMALLINT();
    private static final DataType TINYINT = DataTypes.TINYINT();
    private static final DataType INT = DataTypes.INT();
    private static final DataType BIGINT = DataTypes.BIGINT();
    private static final DataType DECIMAL =
            DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
    private static final DataType FLOAT = DataTypes.FLOAT();
    private static final DataType DOUBLE = DataTypes.DOUBLE();

    private static final DataType TIMESTAMP_TZ =
            DataTypes.TIMESTAMP_TZ(ZonedTimestampType.MAX_PRECISION);
    private static final DataType TIMESTAMP_LTZ =
            DataTypes.TIMESTAMP_LTZ(LocalZonedTimestampType.MAX_PRECISION);
    private static final DataType TIMESTAMP = DataTypes.TIMESTAMP(TimestampType.MAX_PRECISION);
    private static final DataType DATE = DataTypes.DATE();
    private static final DataType TIME = DataTypes.TIME();

    private static final DataType ROW = DataTypes.ROW(INT, STRING);
    private static final DataType ARRAY = DataTypes.ARRAY(STRING);
    private static final DataType MAP = DataTypes.MAP(INT, STRING);

    private static final List<DataType> ALL_TYPES =
            Arrays.asList(
                    // Binary types
                    STRING,
                    CHAR,
                    VARCHAR,
                    BINARY,
                    VARBINARY,
                    // Exact numeric types
                    TINYINT,
                    SMALLINT,
                    INT,
                    BIGINT,
                    DECIMAL,
                    // Inexact numeric types
                    FLOAT,
                    DOUBLE,
                    // Date and time types
                    TIMESTAMP,
                    TIMESTAMP_LTZ,
                    TIMESTAMP_TZ,
                    TIME,
                    // Complex types
                    ROW,
                    ARRAY,
                    MAP);

    private static final Map<DataType, Object> DUMMY_OBJECTS =
            ImmutableMap.of(
                    TINYINT,
                    (byte) 17,
                    SMALLINT,
                    (short) 17,
                    INT,
                    17,
                    BIGINT,
                    17L,
                    DECIMAL,
                    decOf(17),
                    FLOAT,
                    17.0f,
                    DOUBLE,
                    17.0);

    @Test
    void testIsSchemaCompatible() {
        Assertions.assertThat(isSchemaCompatible(null, of("id", BIGINT, "name", VARCHAR(17))))
                .as("test merging into an empty schema")
                .isFalse();

        Assertions.assertThat(
                        isSchemaCompatible(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test identical schema")
                .isTrue();

        Assertions.assertThat(
                        isSchemaCompatible(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("name", VARCHAR(17), "id", BIGINT)))
                .as("swapping sequence is ok")
                .isTrue();

        Assertions.assertThat(
                        isSchemaCompatible(of("id", BIGINT, "name", VARCHAR(17)), of("id", BIGINT)))
                .as("test a wider upcoming schema")
                .isTrue();

        Assertions.assertThat(
                        isSchemaCompatible(of("id", BIGINT), of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a narrower upcoming schema")
                .isFalse();

        Assertions.assertThat(
                        isSchemaCompatible(
                                of("id", BIGINT, "name", STRING),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a wider typed upcoming schema")
                .isTrue();

        Assertions.assertThat(
                        isSchemaCompatible(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", STRING)))
                .as("test a narrower typed upcoming schema")
                .isFalse();

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                isSchemaCompatible(
                                                        of("id", BIGINT, "number", BIGINT),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into BIGINT", type)
                                        .isTrue());

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                isSchemaCompatible(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", BIGINT)))
                                        .as("test fitting BIGINT into %s", type)
                                        .isFalse());

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, STRING)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                isSchemaCompatible(
                                                        of("id", BIGINT, "number", STRING),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into STRING", type)
                                        .isTrue());

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                isSchemaCompatible(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", STRING)))
                                        .as("test fitting STRING into %s", type)
                                        .isFalse());

        Stream.of(FLOAT, DOUBLE, STRING)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                isSchemaCompatible(
                                                        of("id", BIGINT, "number", STRING),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into STRING", type)
                                        .isTrue());

        Stream.of(FLOAT, DOUBLE)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                isSchemaCompatible(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", STRING)))
                                        .as("test fitting STRING into %s", type)
                                        .isFalse());

        Assertions.assertThat(
                        isSchemaCompatible(
                                of("id", BIGINT, "foo", INT), of("id", BIGINT, "bar", INT)))
                .as("columns with different names")
                .isFalse();
    }

    @Test
    void testGetLeastCommonSchema() {
        Assertions.assertThat(getLeastCommonSchema(null, of("id", BIGINT, "name", VARCHAR(17))))
                .as("test merging into an empty schema")
                .isEqualTo(of("id", BIGINT, "name", VARCHAR(17)));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test identical schema")
                .isEqualTo(of("id", BIGINT, "name", VARCHAR(17)));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("name", VARCHAR(17), "id", BIGINT)))
                .as("swapping sequence is ok")
                .isEqualTo(of("id", BIGINT, "name", VARCHAR(17)));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "name", VARCHAR(17)), of("id", BIGINT)))
                .as("test a wider upcoming schema")
                .isEqualTo(of("id", BIGINT, "name", VARCHAR(17)));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT), of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a narrower upcoming schema")
                .isEqualTo(of("id", BIGINT, "name", VARCHAR(17)));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "name", STRING),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a wider typed upcoming schema")
                .isEqualTo(of("id", BIGINT, "name", STRING));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", STRING)))
                .as("test a narrower typed upcoming schema")
                .isEqualTo(of("id", BIGINT, "name", STRING));

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                getLeastCommonSchema(
                                                        of("id", BIGINT, "number", BIGINT),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into BIGINT", type)
                                        .isEqualTo(of("id", BIGINT, "number", BIGINT)));

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                getLeastCommonSchema(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", BIGINT)))
                                        .as("test fitting BIGINT into %s", type)
                                        .isEqualTo(of("id", BIGINT, "number", BIGINT)));

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, STRING)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                getLeastCommonSchema(
                                                        of("id", BIGINT, "number", STRING),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into STRING", type)
                                        .isEqualTo(of("id", BIGINT, "number", STRING)));

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, STRING)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                getLeastCommonSchema(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", STRING)))
                                        .as("test fitting STRING into %s", type)
                                        .isEqualTo(of("id", BIGINT, "number", STRING)));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "foo", INT), of("id", BIGINT, "bar", INT)))
                .as("columns with different names")
                .isEqualTo(of("id", BIGINT, "foo", INT, "bar", INT));

        Assertions.assertThat(
                        getLeastCommonSchema(
                                of("id", BIGINT, "foo", INT, "baz", FLOAT),
                                of("id", BIGINT, "bar", INT, "baz", DOUBLE)))
                .as("mixed schema differences")
                .isEqualTo(of("id", BIGINT, "foo", INT, "baz", DOUBLE, "bar", INT));
    }

    @Test
    void testGetSchemaDifference() {
        Assertions.assertThat(
                        getSchemaDifference(TABLE_ID, null, of("id", BIGINT, "name", VARCHAR(17))))
                .as("test merging into an empty schema")
                .containsExactly(
                        new CreateTableEvent(TABLE_ID, of("id", BIGINT, "name", VARCHAR(17))));

        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID,
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test identical schema")
                .isEmpty();

        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID,
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("name", VARCHAR(17), "id", BIGINT)))
                .as("swapping sequence is ok")
                .isEmpty();

        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID, of("id", BIGINT), of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a widening upcoming schema")
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("name", VARCHAR(17)),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "id"))));

        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID, of("id", BIGINT), of("name", VARCHAR(17), "id", BIGINT)))
                .as("test a widening upcoming schema at first")
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("name", VARCHAR(17)),
                                                AddColumnEvent.ColumnPosition.FIRST,
                                                null))));

        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID,
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", STRING)))
                .as("test a type-widening typed upcoming schema")
                .containsExactly(
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                Collections.singletonMap("name", STRING),
                                Collections.singletonMap("name", VARCHAR(17))));
        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID,
                                of("id", BIGINT, "name", STRING, "number", BIGINT),
                                of("id", BIGINT)))
                .as("test remove id while add gentle")
                .containsExactly(new DropColumnEvent(TABLE_ID, Arrays.asList("number", "name")));
        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID,
                                of("id", BIGINT, "name", STRING, "number", BIGINT),
                                of("id", BIGINT, "name", STRING, "gentle", STRING)))
                .as("test remove id while add gentle")
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("gentle", STRING),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "name"))),
                        new DropColumnEvent(TABLE_ID, Collections.singletonList("number")));
        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                getSchemaDifference(
                                                        TABLE_ID,
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", BIGINT)))
                                        .as("test escalating %s to BIGINT", type)
                                        .containsExactly(
                                                new AlterColumnTypeEvent(
                                                        TABLE_ID,
                                                        Collections.singletonMap("number", BIGINT),
                                                        Collections.singletonMap("number", type))));

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, FLOAT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                getSchemaDifference(
                                                        TABLE_ID,
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", DOUBLE)))
                                        .as("test escalating %s to DOUBLE", type)
                                        .containsExactly(
                                                new AlterColumnTypeEvent(
                                                        TABLE_ID,
                                                        Collections.singletonMap("number", DOUBLE),
                                                        Collections.singletonMap("number", type))));

        Assertions.assertThat(
                        getSchemaDifference(
                                TABLE_ID,
                                of("id", BIGINT, "foo", INT, "baz", FLOAT),
                                of("id", BIGINT, "foo", BIGINT, "bar", INT, "baz", DOUBLE)))
                .as("mixed schema differences")
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("bar", INT),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "foo"))),
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                ImmutableMap.of("foo", BIGINT, "baz", DOUBLE),
                                ImmutableMap.of("foo", INT, "baz", FLOAT)));
    }

    @Test
    void testMergeAndDiff() {
        Assertions.assertThat(mergeAndDiff(null, of("id", BIGINT, "name", VARCHAR(17))))
                .as("test merging into an empty schema")
                .containsExactly(
                        new CreateTableEvent(TABLE_ID, of("id", BIGINT, "name", VARCHAR(17))));

        Assertions.assertThat(
                        mergeAndDiff(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test identical schema")
                .isEmpty();

        Assertions.assertThat(
                        mergeAndDiff(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("name", VARCHAR(17), "id", BIGINT)))
                .as("swapping sequence is ok")
                .isEmpty();

        Assertions.assertThat(mergeAndDiff(of("id", BIGINT, "name", VARCHAR(17)), of("id", BIGINT)))
                .as("test a wider upcoming schema")
                .isEmpty();

        Assertions.assertThat(mergeAndDiff(of("id", BIGINT), of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a narrower upcoming schema")
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("name", VARCHAR(17)),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "id"))));

        Assertions.assertThat(
                        mergeAndDiff(
                                of("id", BIGINT, "name", STRING),
                                of("id", BIGINT, "name", VARCHAR(17))))
                .as("test a wider typed upcoming schema")
                .isEmpty();

        Assertions.assertThat(
                        mergeAndDiff(
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", STRING)))
                .as("test a narrower typed upcoming schema")
                .containsExactly(
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                Collections.singletonMap("name", STRING),
                                Collections.singletonMap("name", VARCHAR(17))));

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                mergeAndDiff(
                                                        of("id", BIGINT, "number", BIGINT),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into BIGINT", type)
                                        .isEmpty());

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                mergeAndDiff(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", BIGINT)))
                                        .as("test fitting BIGINT into %s", type)
                                        .containsExactly(
                                                new AlterColumnTypeEvent(
                                                        TABLE_ID,
                                                        Collections.singletonMap("number", BIGINT),
                                                        Collections.singletonMap("number", type))));

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, STRING)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                mergeAndDiff(
                                                        of("id", BIGINT, "number", STRING),
                                                        of("id", BIGINT, "number", type)))
                                        .as("test fitting %s into STRING", type)
                                        .isEmpty());

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                mergeAndDiff(
                                                        of("id", BIGINT, "number", type),
                                                        of("id", BIGINT, "number", STRING)))
                                        .as("test fitting STRING into %s", type)
                                        .containsExactly(
                                                new AlterColumnTypeEvent(
                                                        TABLE_ID,
                                                        Collections.singletonMap("number", STRING),
                                                        Collections.singletonMap("number", type))));

        Assertions.assertThat(
                        mergeAndDiff(
                                of("id", BIGINT, "foo", INT, "baz", FLOAT),
                                of("id", BIGINT, "bar", INT, "baz", DOUBLE)))
                .as("mixed schema differences")
                .containsExactly(
                        new AddColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("bar", INT),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "baz"))),
                        new AlterColumnTypeEvent(
                                TABLE_ID,
                                Collections.singletonMap("baz", DOUBLE),
                                Collections.singletonMap("baz", FLOAT)));
    }

    @Test
    void testIsDataTypeCompatible() {
        List<Tuple2<DataType, DataType>> viableConversions =
                Arrays.asList(
                        Tuple2.of(CHAR, STRING),
                        Tuple2.of(VARCHAR, STRING),
                        Tuple2.of(BOOLEAN, STRING),
                        Tuple2.of(BINARY, STRING),
                        Tuple2.of(DOUBLE, STRING),
                        Tuple2.of(FLOAT, STRING),
                        Tuple2.of(DECIMAL, STRING),
                        Tuple2.of(BIGINT, STRING),
                        Tuple2.of(INT, STRING),
                        Tuple2.of(SMALLINT, STRING),
                        Tuple2.of(TINYINT, STRING),
                        Tuple2.of(TIMESTAMP_TZ, STRING),
                        Tuple2.of(TIMESTAMP_LTZ, STRING),
                        Tuple2.of(TIMESTAMP, STRING),
                        Tuple2.of(DATE, STRING),
                        Tuple2.of(TIME, STRING),
                        Tuple2.of(ROW, STRING),
                        Tuple2.of(ARRAY, STRING),
                        Tuple2.of(MAP, STRING),
                        Tuple2.of(TINYINT, SMALLINT),
                        Tuple2.of(SMALLINT, INT),
                        Tuple2.of(INT, BIGINT),
                        Tuple2.of(BIGINT, DECIMAL),
                        Tuple2.of(DECIMAL, STRING),
                        Tuple2.of(FLOAT, DOUBLE),
                        Tuple2.of(DATE, TIMESTAMP),
                        Tuple2.of(TIMESTAMP, TIMESTAMP_LTZ),
                        Tuple2.of(TIMESTAMP_LTZ, TIMESTAMP_TZ));

        List<Tuple2<DataType, DataType>> infeasibleConversions =
                Arrays.asList(
                        Tuple2.of(CHAR, BOOLEAN),
                        Tuple2.of(BOOLEAN, BINARY),
                        Tuple2.of(BINARY, DOUBLE),
                        Tuple2.of(DOUBLE, TIMESTAMP_TZ),
                        Tuple2.of(TIMESTAMP_TZ, TIME),
                        Tuple2.of(TIME, ROW),
                        Tuple2.of(ROW, ARRAY),
                        Tuple2.of(ARRAY, MAP));

        viableConversions.forEach(
                conv ->
                        Assertions.assertThat(isDataTypeCompatible(conv.f1, conv.f0))
                                .as("test fitting %s into %s", conv.f0, conv.f1)
                                .isTrue());

        viableConversions.forEach(
                conv ->
                        Assertions.assertThat(isDataTypeCompatible(conv.f0, conv.f1))
                                .as("test fitting %s into %s", conv.f1, conv.f0)
                                .isFalse());

        infeasibleConversions.forEach(
                conv ->
                        Assertions.assertThat(isDataTypeCompatible(conv.f1, conv.f0))
                                .as("test fitting %s into %s", conv.f0, conv.f1)
                                .isFalse());

        infeasibleConversions.forEach(
                conv ->
                        Assertions.assertThat(isDataTypeCompatible(conv.f0, conv.f1))
                                .as("test fitting %s into %s", conv.f1, conv.f0)
                                .isFalse());
    }

    @Test
    void testCoerceObject() {
        Stream<Tuple4<DataType, Object, DataType, Object>> conversionExpects =
                Stream.of(
                        // From TINYINT
                        Tuple4.of(TINYINT, (byte) 0, TINYINT, (byte) 0),
                        Tuple4.of(TINYINT, (byte) 1, SMALLINT, (short) 1),
                        Tuple4.of(TINYINT, (byte) 2, INT, 2),
                        Tuple4.of(TINYINT, (byte) 3, BIGINT, 3L),
                        Tuple4.of(TINYINT, (byte) 4, DECIMAL, decOf(4)),
                        Tuple4.of(TINYINT, (byte) 5, FLOAT, 5.0f),
                        Tuple4.of(TINYINT, (byte) 6, DOUBLE, 6.0),
                        Tuple4.of(TINYINT, (byte) 7, STRING, binStrOf("7")),

                        // From SMALLINT
                        Tuple4.of(SMALLINT, (short) 1, SMALLINT, (short) 1),
                        Tuple4.of(SMALLINT, (short) 2, INT, 2),
                        Tuple4.of(SMALLINT, (short) 3, BIGINT, 3L),
                        Tuple4.of(SMALLINT, (short) 4, DECIMAL, decOf(4)),
                        Tuple4.of(SMALLINT, (short) 5, FLOAT, 5.0f),
                        Tuple4.of(SMALLINT, (short) 6, DOUBLE, 6.0),
                        Tuple4.of(SMALLINT, (short) 7, STRING, binStrOf("7")),

                        // From INT
                        Tuple4.of(INT, 2, INT, 2),
                        Tuple4.of(INT, 3, BIGINT, 3L),
                        Tuple4.of(INT, 4, DECIMAL, decOf(4)),
                        Tuple4.of(INT, 5, FLOAT, 5.0f),
                        Tuple4.of(INT, 6, DOUBLE, 6.0),
                        Tuple4.of(INT, 7, STRING, binStrOf("7")),

                        // From BIGINT
                        Tuple4.of(BIGINT, 3L, BIGINT, 3L),
                        Tuple4.of(BIGINT, 4L, DECIMAL, decOf(4)),
                        Tuple4.of(BIGINT, 5L, FLOAT, 5.0f),
                        Tuple4.of(BIGINT, 6L, DOUBLE, 6.0),
                        Tuple4.of(BIGINT, 7L, STRING, binStrOf("7")),

                        // From DECIMAL
                        Tuple4.of(DECIMAL, decOf(4), DECIMAL, decOf(4)),
                        Tuple4.of(DECIMAL, decOf(5), FLOAT, 5.0f),
                        Tuple4.of(DECIMAL, decOf(6), DOUBLE, 6.0),
                        Tuple4.of(DECIMAL, decOf(7), STRING, binStrOf("7")),

                        // From FLOAT
                        Tuple4.of(FLOAT, 5.0f, FLOAT, 5.0f),
                        Tuple4.of(FLOAT, 6.0f, DOUBLE, 6.0),
                        Tuple4.of(FLOAT, 7.0f, STRING, binStrOf("7.0")),

                        // From DOUBLE
                        Tuple4.of(DOUBLE, 6.0f, DOUBLE, 6.0),
                        Tuple4.of(DOUBLE, 7.0f, STRING, binStrOf("7.0")),

                        // From STRING
                        Tuple4.of(STRING, binStrOf("AtoZ"), STRING, binStrOf("AtoZ")),
                        Tuple4.of(STRING, binStrOf("lie"), STRING, binStrOf("lie")),

                        // From CHAR
                        Tuple4.of(
                                CHAR, binStrOf("les miserables"), CHAR, binStrOf("les miserables")),
                        Tuple4.of(CHAR, binStrOf("notre dame"), STRING, binStrOf("notre dame")),

                        // From Binary
                        Tuple4.of(BINARY, binOf("les miserables"), BINARY, binOf("les miserables")),
                        Tuple4.of(
                                BINARY, binOf("notre dame"), STRING, binStrOf("bm90cmUgZGFtZQ==")),

                        // From BOOLEAN
                        Tuple4.of(BOOLEAN, true, BOOLEAN, true),
                        Tuple4.of(BOOLEAN, false, BOOLEAN, false),
                        Tuple4.of(BOOLEAN, true, STRING, binStrOf("true")),
                        Tuple4.of(BOOLEAN, false, STRING, binStrOf("false")),

                        // From DATE
                        Tuple4.of(DATE, dateOf(2017, 1, 1), DATE, dateOf(2017, 1, 1)),
                        Tuple4.of(DATE, dateOf(2018, 2, 2), TIMESTAMP, tsOf("2018", "02", "02")),
                        Tuple4.of(
                                DATE,
                                dateOf(2019, 3, 3),
                                TIMESTAMP_LTZ,
                                ltzTsOf("2019", "03", "03")),
                        Tuple4.of(
                                DATE, dateOf(2020, 4, 4), TIMESTAMP_TZ, zTsOf("2020", "04", "04")),
                        Tuple4.of(DATE, dateOf(2021, 5, 5), STRING, binStrOf("2021-05-05")),

                        // From TIME
                        Tuple4.of(TIME, timeOf(21, 48, 25), TIME, timeOf(21, 48, 25)),
                        Tuple4.of(TIME, timeOf(21, 48, 25), STRING, binStrOf("21:48:25")),

                        // From TIMESTAMP
                        Tuple4.of(
                                TIMESTAMP,
                                tsOf("2022", "06", "06"),
                                TIMESTAMP,
                                tsOf("2022", "06", "06")),
                        Tuple4.of(
                                TIMESTAMP,
                                tsOf("2023", "07", "07"),
                                TIMESTAMP_LTZ,
                                ltzTsOf("2023", "07", "07")),
                        Tuple4.of(
                                TIMESTAMP,
                                tsOf("2024", "08", "08"),
                                TIMESTAMP_TZ,
                                zTsOf("2024", "08", "08")),
                        Tuple4.of(
                                TIMESTAMP,
                                tsOf("2025", "09", "09"),
                                STRING,
                                binStrOf("2025-09-09T00:00")),

                        // From TIMESTAMP_LTZ
                        Tuple4.of(
                                TIMESTAMP_LTZ,
                                ltzTsOf("2026", "10", "10"),
                                TIMESTAMP_LTZ,
                                ltzTsOf("2026", "10", "10")),
                        Tuple4.of(
                                TIMESTAMP_LTZ,
                                ltzTsOf("2027", "11", "11"),
                                TIMESTAMP_TZ,
                                zTsOf("2027", "11", "11")),
                        Tuple4.of(
                                TIMESTAMP_LTZ,
                                ltzTsOf("2028", "12", "12"),
                                STRING,
                                binStrOf("2028-12-12T00:00")),

                        // From TIMESTAMP_TZ
                        Tuple4.of(
                                TIMESTAMP_TZ,
                                zTsOf("2018", "01", "01"),
                                TIMESTAMP_TZ,
                                zTsOf("2018", "01", "01")),
                        Tuple4.of(
                                TIMESTAMP_TZ,
                                zTsOf("2019", "02", "02"),
                                STRING,
                                binStrOf("2019-02-02T00:00:00Z")));

        conversionExpects.forEach(
                rule ->
                        Assertions.assertThat(coerceObject("UTC", rule.f1, rule.f0, rule.f2))
                                .as("Try coercing %s (%s) to %s type", rule.f1, rule.f0, rule.f2)
                                .isEqualTo(rule.f3));
    }

    @Test
    void testCoerceRow() {
        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", VARCHAR(17)),
                                Arrays.asList(2L, binStrOf("Bob"))))
                .as("test identical schema")
                .containsExactly(2L, binStrOf("Bob"));

        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("name", VARCHAR(17), "id", BIGINT),
                                Arrays.asList(binStrOf("Cecily"), 3L)))
                .as("swapping sequence is ok")
                .containsExactly(3L, binStrOf("Cecily"));

        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT),
                                Collections.singletonList(4L)))
                .as("test a wider upcoming schema")
                .containsExactly(4L, null);

        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", STRING),
                                of("id", BIGINT, "name", VARCHAR(17)),
                                Arrays.asList(4L, "Derrida")))
                .as("test a wider typed upcoming schema")
                .containsExactly(4L, binStrOf("Derrida"));

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                coerceRow(
                                                        "UTC",
                                                        of("id", BIGINT, "number", BIGINT),
                                                        of("id", BIGINT, "number", type),
                                                        Arrays.asList(5L, DUMMY_OBJECTS.get(type))))
                                        .as("test fitting %s into BIGINT", type)
                                        .containsExactly(5L, 17L));

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, FLOAT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                coerceRow(
                                                        "UTC",
                                                        of("id", BIGINT, "number", DOUBLE),
                                                        of("id", BIGINT, "number", type),
                                                        Arrays.asList(6L, DUMMY_OBJECTS.get(type))))
                                        .as("test fitting %s into DOUBLE", type)
                                        .containsExactly(6L, 17.0));

        // Test coercing with NULL
        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT, "name", VARCHAR(17)),
                                Arrays.asList(2L, null)))
                .as("test identical schema")
                .containsExactly(2L, null);

        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("name", VARCHAR(17), "id", BIGINT),
                                Arrays.asList(null, 3L)))
                .as("swapping sequence is ok")
                .containsExactly(3L, null);

        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", VARCHAR(17)),
                                of("id", BIGINT),
                                Collections.singletonList(4L)))
                .as("test a wider upcoming schema")
                .containsExactly(4L, null);

        Assertions.assertThat(
                        coerceRow(
                                "UTC",
                                of("id", BIGINT, "name", STRING),
                                of("id", BIGINT, "name", VARCHAR(17)),
                                Arrays.asList(4L, null)))
                .as("test a wider typed upcoming schema")
                .containsExactly(4L, null);

        Stream.of(TINYINT, SMALLINT, INT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                coerceRow(
                                                        "UTC",
                                                        of("id", BIGINT, "number", BIGINT),
                                                        of("id", BIGINT, "number", type),
                                                        Arrays.asList(5L, null)))
                                        .as("test fitting %s into BIGINT", type)
                                        .containsExactly(5L, null));

        Stream.of(TINYINT, SMALLINT, INT, BIGINT, DECIMAL, FLOAT)
                .forEach(
                        type ->
                                Assertions.assertThat(
                                                coerceRow(
                                                        "UTC",
                                                        of("id", BIGINT, "number", DOUBLE),
                                                        of("id", BIGINT, "number", type),
                                                        Arrays.asList(6L, null)))
                                        .as("test fitting %s into DOUBLE", type)
                                        .containsExactly(6L, null));
    }

    @Test
    void testGetLeastCommonType() {
        // To-be-merged types are:
        // STRING, CHAR, VARCHAR, BINARY, VARBINARY, TINYINT, SMALLINT, INT, BIGINT,
        // DECIMAL, FLOAT, DOUBLE, TIMESTAMP, TIMESTAMP_LTZ, TIMESTAMP_TZ, TIME, ROW, ARRAY,
        // MAP

        assertTypeMergingVector(
                STRING,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                CHAR,
                Arrays.asList(
                        STRING, CHAR, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                VARCHAR,
                Arrays.asList(
                        STRING, STRING, VARCHAR, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                BINARY,
                Arrays.asList(
                        STRING, STRING, STRING, BINARY, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                VARBINARY,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, VARBINARY, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        // 8-bit TINYINT could fit into FLOAT (24 sig bits) or DOUBLE (53 sig bits)
        assertTypeMergingVector(
                TINYINT,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, TINYINT, SMALLINT, INT, BIGINT,
                        DECIMAL, FLOAT, DOUBLE, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        // 16-bit SMALLINT could fit into FLOAT (24 sig bits) or DOUBLE (53 sig bits)
        assertTypeMergingVector(
                SMALLINT,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, SMALLINT, SMALLINT, INT, BIGINT,
                        DECIMAL, FLOAT, DOUBLE, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        // 32-bit INT could fit into DOUBLE (53 sig bits)
        assertTypeMergingVector(
                INT,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, INT, INT, INT, BIGINT, DECIMAL,
                        DOUBLE, DOUBLE, STRING, STRING, STRING, STRING, STRING, STRING, STRING));

        assertTypeMergingVector(
                BIGINT,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, BIGINT, BIGINT, BIGINT, BIGINT,
                        DECIMAL, DOUBLE, DOUBLE, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                DECIMAL,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, DECIMAL, DECIMAL, DECIMAL, DECIMAL,
                        DECIMAL, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                FLOAT,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, FLOAT, FLOAT, DOUBLE, DOUBLE,
                        STRING, FLOAT, DOUBLE, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                DOUBLE,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
                        STRING, DOUBLE, DOUBLE, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                TIMESTAMP,
                Arrays.asList(
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        TIMESTAMP,
                        TIMESTAMP_LTZ,
                        TIMESTAMP_TZ,
                        STRING,
                        STRING,
                        STRING,
                        STRING));

        assertTypeMergingVector(
                TIMESTAMP_LTZ,
                Arrays.asList(
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        TIMESTAMP_LTZ,
                        TIMESTAMP_LTZ,
                        TIMESTAMP_TZ,
                        STRING,
                        STRING,
                        STRING,
                        STRING));

        assertTypeMergingVector(
                TIMESTAMP_TZ,
                Arrays.asList(
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        STRING,
                        TIMESTAMP_TZ,
                        TIMESTAMP_TZ,
                        TIMESTAMP_TZ,
                        STRING,
                        STRING,
                        STRING,
                        STRING));

        assertTypeMergingVector(
                TIME,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, TIME, STRING, STRING,
                        STRING));

        assertTypeMergingVector(
                ROW,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, ROW, STRING,
                        STRING));

        assertTypeMergingVector(
                ARRAY,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, ARRAY,
                        STRING));

        assertTypeMergingVector(
                MAP,
                Arrays.asList(
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        MAP));
    }

    private static void assertTypeMergingVector(DataType incomingType, List<DataType> resultTypes) {
        Assertions.assertThat(ALL_TYPES)
                .map(type -> getLeastCommonType(type, incomingType))
                .containsExactlyElementsOf(resultTypes)
                // Flip LHS and RHS should emit same outputs
                .map(type -> getLeastCommonType(incomingType, type))
                .containsExactlyElementsOf(resultTypes);
    }

    // Some testing utility methods.

    private static List<SchemaChangeEvent> mergeAndDiff(
            @Nullable Schema currentSchema, Schema upcomingSchema) {
        Schema afterSchema = getLeastCommonSchema(currentSchema, upcomingSchema);
        return getSchemaDifference(TABLE_ID, currentSchema, afterSchema);
    }

    private static Schema of(Object... args) {
        List<Object> argList = new ArrayList<>(Arrays.asList(args));
        Preconditions.checkState(argList.size() % 2 == 0);
        Schema.Builder builder = Schema.newBuilder();
        while (!argList.isEmpty()) {
            String colName = (String) argList.remove(0);
            DataType colType = (DataType) argList.remove(0);
            builder.physicalColumn(colName, colType);
        }
        return builder.build();
    }

    private static DateData dateOf(int year, int month, int dayOfMonth) {
        return DateData.fromLocalDate(LocalDate.of(year, month, dayOfMonth));
    }

    private static TimeData timeOf(int hour, int minute, int second) {
        return TimeData.fromLocalTime(LocalTime.of(hour, minute, second));
    }

    private static TimestampData tsOf(String year, String month, String dayOfMonth) {
        return TimestampData.fromTimestamp(
                Timestamp.valueOf(String.format("%s-%s-%s 00:00:00", year, month, dayOfMonth)));
    }

    private static LocalZonedTimestampData ltzTsOf(String year, String month, String dayOfMonth) {
        return LocalZonedTimestampData.fromEpochMillis(
                Instant.parse(String.format("%s-%s-%sT00:00:00Z", year, month, dayOfMonth))
                        .toEpochMilli());
    }

    private static ZonedTimestampData zTsOf(String year, String month, String dayOfMonth) {
        return ZonedTimestampData.fromZonedDateTime(
                ZonedDateTime.ofInstant(
                        Instant.parse(String.format("%s-%s-%sT00:00:00Z", year, month, dayOfMonth)),
                        ZoneId.of("UTC")));
    }

    private static DecimalData decOf(long value) {
        return DecimalData.fromBigDecimal(
                BigDecimal.valueOf(value), DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE);
    }

    private static BinaryStringData binStrOf(String str) {
        return BinaryStringData.fromString(str);
    }

    private static byte[] binOf(String str) {
        return str.getBytes();
    }
}
