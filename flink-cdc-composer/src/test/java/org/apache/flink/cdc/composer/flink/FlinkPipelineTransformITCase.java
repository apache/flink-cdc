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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.codehaus.commons.compiler.CompileException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Integration test for {@link FlinkPipelineComposer}. */
class FlinkPipelineTransformITCase {

    private static final int MAX_PARALLELISM = 4;

    // Always use parent-first classloader for CDC classes.
    // The reason is that ValuesDatabase uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void init() {
        // Take over STDOUT as we need to check the output of values sink
        System.setOut(new PrintStream(outCaptor));
        // Initialize in-memory database
        ValuesDatabase.clear();
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
    }

    /** This tests if we can append calculated columns based on existing columns. */
    @ParameterizedTest
    @EnumSource
    void testCalculatedColumns(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, id || name AS uid, age * 2 AS double_age",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`uid` STRING,`double_age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, 1Alice, 36], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, 2Bob, 40], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, 2Bob, 40], after=[2, Bob, 30, 2Bob, 60], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`uid` STRING,`double_age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, 3Carol, 30], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, 4Derrida, 50], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, 4Derrida, 50], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if we can reference a column more than once in projection expressions. */
    @ParameterizedTest
    @EnumSource
    void testMultipleReferencedColumnsInProjection(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, CAST(age * age * age AS INT) AS cubic_age",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`cubic_age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, 5832], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, 8000], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, 8000], after=[2, Bob, 30, 27000], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`cubic_age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, 3375], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, 15625], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, 15625], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if we can reference a column more than once in filtering expressions. */
    @ParameterizedTest
    @EnumSource
    void testMultipleReferencedColumnsInFilter(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                null,
                                "id > 2 AND id < 4",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student], op=INSERT, meta=()}"));
    }

    /** This tests if we can filter out source records by rule. */
    @ParameterizedTest
    @EnumSource
    void testFilteringRules(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                null,
                                "CHAR_LENGTH(name) > 3",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student], after=[], op=DELETE, meta=()}"));
    }

    /**
     * This tests if transform rule could be used to classify source records based on filtering
     * rules.
     */
    @ParameterizedTest
    @EnumSource
    void testMultipleDispatchTransform(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Arrays.asList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, 'YOUNG' AS category",
                                "age < 20",
                                null,
                                null,
                                null,
                                null,
                                null),
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, 'OLD' AS category",
                                "age >= 20",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`category` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, YOUNG], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, OLD], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, OLD], after=[2, Bob, 30, OLD], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`category` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, YOUNG], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, OLD], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, OLD], after=[], op=DELETE, meta=()}"));
    }

    @ParameterizedTest
    @EnumSource
    void testMultipleTransformWithDiffRefColumn(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Arrays.asList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "id,age,'Juvenile' AS roleName",
                                "age < 18",
                                null,
                                null,
                                null,
                                null,
                                null),
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "id,age,name AS roleName",
                                "age >= 18",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`age` INT,`roleName` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, 18, Alice], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, 20, Bob], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, 20, Bob], after=[2, 30, Bob], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`age` TINYINT,`roleName` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, 15, Juvenile], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, 25, Derrida], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, 25, Derrida], after=[], op=DELETE, meta=()}"));
    }

    @ParameterizedTest
    @EnumSource
    void testMultiTransformWithAsterisk(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Arrays.asList(
                        new TransformDef(
                                "default_namespace.default_schema.mytable2",
                                "*,'Juvenile' AS roleName",
                                "age < 18",
                                null,
                                null,
                                null,
                                null,
                                null),
                        new TransformDef(
                                "default_namespace.default_schema.mytable2",
                                "id,name,age,description,name AS roleName",
                                "age >= 18",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`roleName` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, Juvenile], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, Derrida], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, Derrida], after=[], op=DELETE, meta=()}"));
    }

    @ParameterizedTest
    @EnumSource
    void testMultiTransformMissingProjection(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Arrays.asList(
                        new TransformDef(
                                "default_namespace.default_schema.mytable2",
                                null,
                                "age < 18",
                                null,
                                null,
                                null,
                                null,
                                null),
                        new TransformDef(
                                "default_namespace.default_schema.mytable2",
                                "id,UPPER(name) AS name,age,description",
                                "age >= 18",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` STRING,`age` TINYINT,`description` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, DERRIDA, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, DERRIDA, 25, student], after=[], op=DELETE, meta=()}"));
    }

    @ParameterizedTest
    @EnumSource
    @Disabled("to be fixed in FLINK-37132")
    void testMultiTransformSchemaColumnsCompatibilityWithNullProjection(
            ValuesDataSink.SinkApi sinkApi) {
        TransformDef nullProjection =
                new TransformDef(
                        "default_namespace.default_schema.mytable2",
                        null,
                        "age < 18",
                        null,
                        null,
                        null,
                        null,
                        null);

        assertThatThrownBy(
                        () ->
                                runGenericTransformTest(
                                        sinkApi,
                                        Arrays.asList(
                                                nullProjection,
                                                new TransformDef(
                                                        "default_namespace.default_schema.mytable2",
                                                        // reference part column
                                                        "id,UPPER(name) AS name",
                                                        "age >= 18",
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null)),
                                        Collections.emptyList()))
                .rootCause()
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Unable to merge schema columns={`id` BIGINT,`name` VARCHAR(255),`age` TINYINT,`description` STRING}, primaryKeys=id, options=() "
                                + "and columns={`id` BIGINT,`name` STRING}, primaryKeys=id, options=() with different column counts.");
    }

    @ParameterizedTest
    @EnumSource
    @Disabled("to be fixed in FLINK-37132")
    void testMultiTransformSchemaColumnsCompatibilityWithEmptyProjection(
            ValuesDataSink.SinkApi sinkApi) {
        TransformDef emptyProjection =
                new TransformDef(
                        "default_namespace.default_schema.mytable2",
                        "",
                        "age < 18",
                        null,
                        null,
                        null,
                        null,
                        null);

        assertThatThrownBy(
                        () ->
                                runGenericTransformTest(
                                        sinkApi,
                                        Arrays.asList(
                                                emptyProjection,
                                                new TransformDef(
                                                        "default_namespace.default_schema.mytable2",
                                                        // reference part column
                                                        "id,UPPER(name) AS name",
                                                        "age >= 18",
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null)),
                                        Collections.emptyList()))
                .rootCause()
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Unable to merge schema columns={`id` BIGINT,`name` VARCHAR(255),`age` TINYINT,`description` STRING}, primaryKeys=id, options=() "
                                + "and columns={`id` BIGINT,`name` STRING}, primaryKeys=id, options=() with different column counts.");
    }

    @ParameterizedTest
    @EnumSource
    void testMultiTransformWithNullEmptyAsteriskProjections(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        TransformDef nullProjection =
                new TransformDef(
                        "default_namespace.default_schema.mytable2",
                        null,
                        "age < 18",
                        null,
                        null,
                        null,
                        null,
                        null);

        TransformDef emptyProjection =
                new TransformDef(
                        "default_namespace.default_schema.mytable2",
                        "",
                        "age < 18",
                        null,
                        null,
                        null,
                        null,
                        null);

        TransformDef asteriskProjection =
                new TransformDef(
                        "default_namespace.default_schema.mytable2",
                        "*",
                        "age < 18",
                        null,
                        null,
                        null,
                        null,
                        null);

        runGenericTransformTest(
                sinkApi,
                Arrays.asList(
                        // Setting projection as null, '', or * should be equivalent
                        nullProjection,
                        emptyProjection,
                        asteriskProjection,
                        new TransformDef(
                                "default_namespace.default_schema.mytable2",
                                // reference all column
                                "id,UPPER(name) AS name,age,description",
                                "age >= 18",
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` STRING,`age` TINYINT,`description` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, DERRIDA, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, DERRIDA, 25, student], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if transform generates metadata info correctly. */
    @ParameterizedTest
    @EnumSource
    void testMetadataInfo(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*",
                                null,
                                "id,name",
                                "id",
                                "replication_num=1,bucket=17",
                                "Just a Transform Block",
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING NOT NULL,`age` INT}, primaryKeys=id;name, partitionKeys=id, options=({bucket=17, replication_num=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255) NOT NULL,`age` TINYINT,`description` STRING}, primaryKeys=id;name, partitionKeys=id, options=({bucket=17, replication_num=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student], after=[], op=DELETE, meta=()}"));
    }

    /**
     * This tests if transform generates metadata info correctly without specifying projection /
     * filtering rules.
     */
    @ParameterizedTest
    @EnumSource
    void testMetadataInfoWithoutChangingSchema(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                null,
                                null,
                                "id,name",
                                "id",
                                "replication_num=1,bucket=17",
                                "A Transform Block without projection or filter",
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id;name, partitionKeys=id, options=({bucket=17, replication_num=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT,`name` VARCHAR(255),`age` TINYINT,`description` STRING}, primaryKeys=id;name, partitionKeys=id, options=({bucket=17, replication_num=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if projection rule could reference metadata info correctly. */
    @ParameterizedTest
    @EnumSource
    void testMetadataColumn(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "id, name, age, __namespace_name__, __schema_name__, __table_name__",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`__namespace_name__` STRING NOT NULL,`__schema_name__` STRING NOT NULL,`__table_name__` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, default_namespace, default_schema, mytable1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, default_namespace, default_schema, mytable1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, default_namespace, default_schema, mytable1], after=[2, Bob, 30, default_namespace, default_schema, mytable1], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`__namespace_name__` STRING NOT NULL,`__schema_name__` STRING NOT NULL,`__table_name__` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, default_namespace, default_schema, mytable2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, default_namespace, default_schema, mytable2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, default_namespace, default_schema, mytable2], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if projection rule could reference metadata info correctly with wildcard (*). */
    @ParameterizedTest
    @EnumSource
    void testMetadataColumnWithWildcard(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, __namespace_name__, __schema_name__, __table_name__",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`__namespace_name__` STRING NOT NULL,`__schema_name__` STRING NOT NULL,`__table_name__` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, default_namespace, default_schema, mytable1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, default_namespace, default_schema, mytable1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, default_namespace, default_schema, mytable1], after=[2, Bob, 30, default_namespace, default_schema, mytable1], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`__namespace_name__` STRING NOT NULL,`__schema_name__` STRING NOT NULL,`__table_name__` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, default_namespace, default_schema, mytable2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, default_namespace, default_schema, mytable2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, default_namespace, default_schema, mytable2], after=[], op=DELETE, meta=()}"));
    }

    /**
     * This tests if transform operator could distinguish metadata column identifiers and string
     * literals.
     */
    @ParameterizedTest
    @EnumSource
    void testUsingMetadataColumnLiteralWithWildcard(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, '__namespace_name____schema_name____table_name__' AS string_literal",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`string_literal` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, __namespace_name____schema_name____table_name__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, __namespace_name____schema_name____table_name__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, __namespace_name____schema_name____table_name__], after=[2, Bob, 30, __namespace_name____schema_name____table_name__], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`string_literal` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, __namespace_name____schema_name____table_name__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, __namespace_name____schema_name____table_name__], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, __namespace_name____schema_name____table_name__], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if projection rule could reference metadata info correctly. */
    @ParameterizedTest
    @EnumSource
    void testConvertDeleteAsInsert(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "id, name, age, __namespace_name__, __schema_name__, __table_name__, __data_event_type__",
                                null,
                                null,
                                null,
                                null,
                                null,
                                "SOFT_DELETE")),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`__namespace_name__` STRING NOT NULL,`__schema_name__` STRING NOT NULL,`__table_name__` STRING NOT NULL,`__data_event_type__` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, default_namespace, default_schema, mytable1, +I], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, default_namespace, default_schema, mytable1, +I], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, default_namespace, default_schema, mytable1, -U], after=[2, Bob, 30, default_namespace, default_schema, mytable1, +U], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`__namespace_name__` STRING NOT NULL,`__schema_name__` STRING NOT NULL,`__table_name__` STRING NOT NULL,`__data_event_type__` STRING NOT NULL}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, default_namespace, default_schema, mytable2, +I], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, default_namespace, default_schema, mytable2, +I], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, default_namespace, default_schema, mytable2, -D], op=INSERT, meta=()}"));
    }

    /** This tests if built-in comparison functions work as expected. */
    @ParameterizedTest
    @EnumSource
    void testBuiltinComparisonFunctions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, "
                                        + "id = 2 AS col1, id <> 3 AS col2, id > 2 as col3, "
                                        + "id >= 2 as col4, id < 3 as col5, id <= 4 as col6, "
                                        + "name IS NULL as col7, name IS NOT NULL as col8, "
                                        + "id BETWEEN 1 AND 3 as col9, id NOT BETWEEN 2 AND 4 as col10, "
                                        + "name LIKE 'li' as col11, name LIKE 'ro' as col12, "
                                        + "CAST(id AS INT) IN (1, 3, 5) as col13, name IN ('Bob', 'Derrida') AS col14",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`col1` BOOLEAN,`col2` BOOLEAN,`col3` BOOLEAN,`col4` BOOLEAN,`col5` BOOLEAN,`col6` BOOLEAN,`col7` BOOLEAN,`col8` BOOLEAN,`col9` BOOLEAN,`col10` BOOLEAN,`col11` BOOLEAN,`col12` BOOLEAN,`col13` BOOLEAN,`col14` BOOLEAN}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, false, true, false, false, true, true, false, true, true, true, true, false, true, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, true, true, false, true, true, true, false, true, true, false, false, false, false, true], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, true, true, false, true, true, true, false, true, true, false, false, false, false, true], after=[2, Bob, 30, true, true, false, true, true, true, false, true, true, false, false, false, false, true], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`col1` BOOLEAN,`col2` BOOLEAN,`col3` BOOLEAN,`col4` BOOLEAN,`col5` BOOLEAN,`col6` BOOLEAN,`col7` BOOLEAN,`col8` BOOLEAN,`col9` BOOLEAN,`col10` BOOLEAN,`col11` BOOLEAN,`col12` BOOLEAN,`col13` BOOLEAN,`col14` BOOLEAN}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, false, true, true, true, false, true, false, true, true, false, false, true, true, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, false, true, true, true, false, true, false, true, false, false, false, false, false, true], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, false, true, true, true, false, true, false, true, false, false, false, false, false, true], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if built-in logical functions work as expected. */
    @ParameterizedTest
    @EnumSource
    void testBuiltinLogicalFunctions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, "
                                        + "id = 2 OR true as col1, id <> 3 OR false as col2, "
                                        + "name = 'Alice' AND true as col4, name <> 'Bob' AND false as col5, "
                                        + "NOT id = 1 as col6, id = 3 IS FALSE as col7, "
                                        + "name = 'Derrida' IS TRUE as col8, "
                                        + "name <> 'Carol' IS NOT FALSE as col9, "
                                        + "name <> 'Eve' IS NOT TRUE as col10",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`col1` BOOLEAN,`col2` BOOLEAN,`col4` BOOLEAN,`col5` BOOLEAN,`col6` BOOLEAN,`col7` BOOLEAN,`col8` BOOLEAN,`col9` BOOLEAN,`col10` BOOLEAN}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, true, true, true, false, false, true, false, true, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, true, true, false, false, true, true, false, true, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, true, true, false, false, true, true, false, true, false], after=[2, Bob, 30, true, true, false, false, true, true, false, true, false], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`col1` BOOLEAN,`col2` BOOLEAN,`col4` BOOLEAN,`col5` BOOLEAN,`col6` BOOLEAN,`col7` BOOLEAN,`col8` BOOLEAN,`col9` BOOLEAN,`col10` BOOLEAN}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, true, true, false, false, true, true, false, false, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, true, true, false, false, true, true, true, true, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, true, true, false, false, true, true, true, true, false], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if built-in arithmetic functions work as expected. */
    @ParameterizedTest
    @EnumSource
    void testBuiltinArithmeticFunctions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, "
                                        + "id + 17 AS col1, id - 17 AS col2, id * 17 AS col3, "
                                        + "CAST(id AS DOUBLE) / 1.7 AS col4, "
                                        + "CAST(id AS INT) % 3 AS col5, ABS(id - 17) AS col6, "
                                        + "CEIL(CAST(id AS DOUBLE) / 1.7) AS col7, "
                                        + "FLOOR(CAST(id AS DOUBLE) / 1.7) AS col8, "
                                        + "ROUND(CAST(id AS DOUBLE) / 1.7, 0) AS col9, "
                                        + "CHAR_LENGTH(UUID()) AS col10",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`col1` INT,`col2` INT,`col3` INT,`col4` DOUBLE,`col5` INT,`col6` INT,`col7` DOUBLE,`col8` DOUBLE,`col9` DOUBLE,`col10` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, 18, -16, 17, 0.5882352941176471, 1, 16, 1.0, 0.0, 1.0, 36], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, 19, -15, 34, 1.1764705882352942, 2, 15, 2.0, 1.0, 1.0, 36], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, 19, -15, 34, 1.1764705882352942, 2, 15, 2.0, 1.0, 1.0, 36], after=[2, Bob, 30, 19, -15, 34, 1.1764705882352942, 2, 15, 2.0, 1.0, 1.0, 36], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`col1` BIGINT,`col2` BIGINT,`col3` BIGINT,`col4` DOUBLE,`col5` INT,`col6` BIGINT,`col7` DOUBLE,`col8` DOUBLE,`col9` DOUBLE,`col10` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, 20, -14, 51, 1.7647058823529411, 0, 14, 2.0, 1.0, 2.0, 36], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, 21, -13, 68, 2.3529411764705883, 1, 13, 3.0, 2.0, 2.0, 36], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, 21, -13, 68, 2.3529411764705883, 1, 13, 3.0, 2.0, 2.0, 36], after=[], op=DELETE, meta=()}"));
    }

    /** This tests if built-in string functions work as expected. */
    @ParameterizedTest
    @EnumSource
    void testBuiltinStringFunctions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, "
                                        + "'Dear ' || name AS col1, "
                                        + "CHAR_LENGTH(name) AS col2, "
                                        + "UPPER(name) AS col3, "
                                        + "LOWER(name) AS col4, "
                                        + "TRIM(name) AS col5, "
                                        + "REGEXP_REPLACE(name, 'Al|Bo', '**') AS col6, "
                                        + "SUBSTR(name, 1, 1) AS col7, "
                                        + "SUBSTR(name, 2, 1) AS col8, "
                                        + "SUBSTR(name, 3) AS col9, "
                                        + "CONCAT(name, ' - ', CAST(id AS VARCHAR)) AS col10",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`col1` STRING,`col2` INT,`col3` STRING,`col4` STRING,`col5` STRING,`col6` STRING,`col7` STRING,`col8` STRING,`col9` STRING,`col10` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, Dear Alice, 5, ALICE, alice, Alice, **ice, A, l, ice, Alice - 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, Dear Bob, 3, BOB, bob, Bob, **b, B, o, b, Bob - 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, Dear Bob, 3, BOB, bob, Bob, **b, B, o, b, Bob - 2], after=[2, Bob, 30, Dear Bob, 3, BOB, bob, Bob, **b, B, o, b, Bob - 2], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`col1` STRING,`col2` INT,`col3` STRING,`col4` STRING,`col5` STRING,`col6` STRING,`col7` STRING,`col8` STRING,`col9` STRING,`col10` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, Dear Carol, 5, CAROL, carol, Carol, Carol, C, a, rol, Carol - 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, Dear Derrida, 7, DERRIDA, derrida, Derrida, Derrida, D, e, rrida, Derrida - 4], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, Dear Derrida, 7, DERRIDA, derrida, Derrida, Derrida, D, e, rrida, Derrida - 4], after=[], op=DELETE, meta=()}"));
    }

    @ParameterizedTest
    @EnumSource
    @Disabled("SUBSTRING ... FROM ... FOR ... isn't available until we close FLINK-35985.")
    void testSubstringFunctions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, "
                                        + "SUBSTR(name, 0, 1) AS col1, "
                                        + "SUBSTR(name, 2, 1) AS col2, "
                                        + "SUBSTR(name, 3) AS col3, "
                                        + "SUBSTRING(name FROM 0 FOR 1) AS col4, "
                                        + "SUBSTRING(name FROM 2 FOR 1) AS col5, "
                                        + "SUBSTRING(name FROM 3) AS col6",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList("To", "be", "added"));
    }

    /** This tests if built-in conditional functions work as expected. */
    @ParameterizedTest
    @EnumSource
    @Disabled("This case will not run until we close FLINK-35986.")
    void testConditionalFunctions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        runGenericTransformTest(
                sinkApi,
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.\\.*",
                                "*, "
                                        + "CASE UPPER(name)"
                                        + "  WHEN 'ALICE' THEN 'A - Alice'"
                                        + "  WHEN 'BOB' THEN 'B - Bob'"
                                        + "  WHEN 'CAROL' THEN 'C - Carol'"
                                        + "  ELSE 'D - Derrida' END AS col1, "
                                        + "CASE"
                                        + "  WHEN id = 1 THEN '1 - One'"
                                        + "  WHEN id = 2 THEN '2 - Two'"
                                        + "  WHEN id = 3 THEN '3 - Three'"
                                        + "  ELSE '4 - Four' END AS col2, "
                                        + "COALESCE(name, 'FALLBACK') AS col3, "
                                        + "COALESCE(NULL, NULL, id, 42, NULL) AS col4, "
                                        + "COALESCE(NULL, NULL, NULL, NULL, NULL) AS col5, "
                                        + "IF(TRUE, 'true', 'false') AS col6, "
                                        + "IF(id < 3, 'ID < 3', 'ID >= 3') AS col7, "
                                        + "IF(name = 'Alice', IF(id = 1, 'YES', 'NO'), 'NO') AS col8",
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)),
                Arrays.asList("Foo", "Bar", "Baz"));
    }

    /** This tests if transform temporal functions works as expected. */
    @Test
    void testTransformWithTemporalFunction() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId myTable1 = TableId.tableId("default_namespace", "default_schema", "mytable1");
        TableId myTable2 = TableId.tableId("default_namespace", "default_schema", "mytable2");
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        Schema table2Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(255))
                        .physicalColumn("age", DataTypes.TINYINT())
                        .physicalColumn("description", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<Event> events = getTestEvents(table1Schema, table2Schema, myTable1, myTable2);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "America/Los_Angeles");
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "default_namespace.default_schema.\\.*",
                                        "*, LOCALTIME as lcl_t, CURRENT_TIME as cur_t, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as cur_ts, CAST(NOW() AS TIMESTAMP) as now_ts, LOCALTIMESTAMP as lcl_ts, CURRENT_DATE as cur_dt",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        Arrays.stream(outputEvents).forEach(this::extractDataLines);
    }

    @ParameterizedTest
    @EnumSource
    public void testTransformWithColumnNameMap(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.COMPLEX_COLUMN_NAME_TABLE);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup transform
        TransformDef transformDef =
                new TransformDef(
                        "default_namespace.default_schema.table1",
                        "*, `timestamp-type`",
                        "`foo-bar` > 0",
                        null,
                        null,
                        null,
                        null,
                        null);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(transformDef),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`class` STRING NOT NULL,`foo-bar` INT,`bar-foo` INT,`timestamp-type` STRING NOT NULL}, primaryKeys=class, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[class1, 1, 10, type1], op=INSERT, meta=({timestamp-type=type1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[class2, 2, 100, type2], op=INSERT, meta=({timestamp-type=type2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`import-package` STRING, position=AFTER, existedColumnName=bar-foo}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={bar-foo=bar-baz}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[bar-baz]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[class1, 1, , type1], after=[], op=DELETE, meta=({timestamp-type=type1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[class2, 2, , type2], after=[new-class2, 20, new-package2, type2], op=UPDATE, meta=({timestamp-type=type2})}");
    }

    /** This tests if transform temporal functions works as expected. */
    @ParameterizedTest
    @ValueSource(strings = {"America/Los_Angeles", "UTC", "Asia/Shanghai"})
    void testTransformWithTimestamps(String timezone) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId myTable = TableId.tableId("default_namespace", "default_schema", "mytable1");
        Schema tableSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("ts", DataTypes.TIMESTAMP(6))
                        .physicalColumn("ts_ltz", DataTypes.TIMESTAMP_LTZ(6))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        tableSchema.getColumnDataTypes().toArray(new DataType[0]));

        List<Event> events =
                Arrays.asList(
                        new CreateTableEvent(myTable, tableSchema),
                        DataChangeEvent.insertEvent(
                                myTable,
                                generator.generate(
                                        new Object[] {
                                            1,
                                            TimestampData.fromTimestamp(
                                                    Timestamp.valueOf("2023-11-27 20:12:31")),
                                            LocalZonedTimestampData.fromInstant(
                                                    toInstant("2020-07-17 18:00:22", timezone)),
                                        })),
                        DataChangeEvent.insertEvent(
                                myTable,
                                generator.generate(
                                        new Object[] {
                                            2,
                                            TimestampData.fromTimestamp(
                                                    Timestamp.valueOf("2018-02-01 04:14:01")),
                                            LocalZonedTimestampData.fromInstant(
                                                    toInstant("2019-12-31 21:00:22", timezone)),
                                        })),
                        DataChangeEvent.insertEvent(
                                myTable, generator.generate(new Object[] {3, null, null})));

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, timezone);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "default_namespace.default_schema.\\.*",
                                        "id, "
                                                + "DATE_FORMAT(ts, 'yyyy~MM~dd') AS df1, "
                                                + "DATE_FORMAT(ts_ltz, 'yyyy~MM~dd') AS df2, "
                                                + "DATE_FORMAT(ts, 'yyyy->MM->dd / HH->mm->ss') AS df3, "
                                                + "DATE_FORMAT(ts_ltz, 'yyyy->MM->dd / HH->mm->ss') AS df4, "
                                                + "DATE_FORMAT(TIMESTAMPADD(SECOND, 17, ts), 'yyyy->MM->dd / HH->mm->ss') AS df5, "
                                                + "DATE_FORMAT(TIMESTAMPADD(SECOND, 17, ts_ltz), 'yyyy->MM->dd / HH->mm->ss') AS df6",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        Assertions.assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`df1` STRING,`df2` STRING,`df3` STRING,`df4` STRING,`df5` STRING,`df6` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, 2023~11~27, 2020~07~17, 2023->11->27 / 20->12->31, 2020->07->17 / 18->00->22, 2023->11->27 / 20->12->48, 2020->07->17 / 18->00->39], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, 2018~02~01, 2019~12~31, 2018->02->01 / 04->14->01, 2019->12->31 / 21->00->22, 2018->02->01 / 04->14->18, 2019->12->31 / 21->00->39], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, null, null, null, null, null, null], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testTransformMergingIncompatibleRules(ValuesDataSink.SinkApi apiVersion) {
        Assertions.assertThatThrownBy(
                        () ->
                                runGenericTransformTest(
                                        apiVersion,
                                        Arrays.asList(
                                                new TransformDef(
                                                        "\\.*.\\.*.mytable1",
                                                        "*, 'rule_1_matched' AS rule_1_matched",
                                                        "id > 0",
                                                        null,
                                                        "id",
                                                        null,
                                                        null,
                                                        null),
                                                new TransformDef(
                                                        "\\.*.\\.*.\\.*",
                                                        "*, 'rule_fallback' AS rule_fallback",
                                                        null,
                                                        null,
                                                        "id",
                                                        null,
                                                        null,
                                                        null)),
                                        Collections.emptyList()))
                .rootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Trying to merge transformed schemas [columns={`id` INT,`name` STRING,`age` INT,`rule_1_matched` STRING}, primaryKeys=id, partitionKeys=id, options=(), columns={`id` INT,`name` STRING,`age` INT,`rule_fallback` STRING}, primaryKeys=id, partitionKeys=id, options=()], but got more than one column name views: [[id, name, age, rule_1_matched], [id, name, age, rule_fallback]]");
    }

    @ParameterizedTest
    @EnumSource
    void testTransformWithFallbackRules(ValuesDataSink.SinkApi apiVersion) throws Exception {
        runGenericTransformTest(
                apiVersion,
                Arrays.asList(
                        new TransformDef(
                                "\\.*.\\.*.mytable1",
                                "*, 'rule_1_matched' AS rule_1_matched",
                                null,
                                null,
                                "id",
                                null,
                                null,
                                null),
                        new TransformDef(
                                "\\.*.\\.*.\\.*",
                                "*, 'rule_fallback' AS rule_fallback",
                                null,
                                null,
                                "id",
                                null,
                                null,
                                null)),
                Arrays.asList(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`rule_1_matched` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, rule_1_matched], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, rule_1_matched], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, rule_1_matched], after=[2, Bob, 30, rule_1_matched], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`rule_fallback` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, rule_fallback], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, rule_fallback], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, rule_fallback], after=[], op=DELETE, meta=()}"));
    }

    void runGenericTransformTest(
            ValuesDataSink.SinkApi sinkApi,
            List<TransformDef> transformDefs,
            List<String> expectedResults)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId myTable1 = TableId.tableId("default_namespace", "default_schema", "mytable1");
        TableId myTable2 = TableId.tableId("default_namespace", "default_schema", "mytable2");
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        Schema table2Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("name", DataTypes.VARCHAR(255))
                        .physicalColumn("age", DataTypes.TINYINT())
                        .physicalColumn("description", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        List<Event> events = getTestEvents(table1Schema, table2Schema, myTable1, myTable2);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        transformDefs,
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents).containsExactly(expectedResults.toArray(new String[0]));
    }

    private static List<Event> getTestEvents(
            Schema table1Schema, Schema table2Schema, TableId myTable1, TableId myTable2) {
        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordDataGenerator table2dataGenerator =
                new BinaryRecordDataGenerator(
                        table2Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {1, BinaryStringData.fromString("Alice"), 18})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob"), 20})));
        events.add(
                DataChangeEvent.updateEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob"), 20}),
                        table1dataGenerator.generate(
                                new Object[] {2, BinaryStringData.fromString("Bob"), 30})));
        events.add(new CreateTableEvent(myTable2, table2Schema));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    3L,
                                    BinaryStringData.fromString("Carol"),
                                    (byte) 15,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Derrida"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Derrida"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        return events;
    }

    @Test
    void testVanillaTransformWithSchemaEvolution() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 21], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23], after=[3, Colin, 24], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=FIRST, existedColumnName=null}, ColumnWithPosition{column=`gender` TINYINT, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1], after=[2nd, 5, Eva, 20, 2], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2], after=[5th, 8, Harry, 18.0, -3], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0], after=[8th, 11, Kella, 18.0, 0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko], after=[11th, 14, Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testWildcardTransformWithSchemaEvolution() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        tableId.toString(),
                                        "*",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 21], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23], after=[3, Colin, 24], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1], after=[2nd, 5, Eva, 20, 2], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2], after=[5th, 8, Harry, 18.0, -3], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0], after=[8th, 11, Kella, 18.0, 0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko], after=[11th, 14, Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testExplicitTransformWithSchemaEvolution() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        tableId.toString(),
                                        "id, name, CAST(id AS VARCHAR) || ' -> ' || name AS extend_id",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`extend_id` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 1 -> Alice], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 2 -> Barcarolle], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 3 -> Cecily], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 3 -> Cecily], after=[3, Colin, 3 -> Colin], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 2 -> Barcarolle], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4, Derrida, 4 -> Derrida], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5, Eve, 5 -> Eve], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5, Eve, 5 -> Eve], after=[5, Eva, 5 -> Eva], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6, Fiona, 6 -> Fiona], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6, Fiona, 6 -> Fiona], after=[], op=DELETE, meta=()}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={name=VARCHAR(17)}, oldTypeMapping={name=STRING}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7, Gem, 7 -> Gem], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8, Helen, 8 -> Helen], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8, Helen, 8 -> Helen], after=[8, Harry, 8 -> Harry], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9, IINA, 9 -> IINA], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9, IINA, 9 -> IINA], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10, Julia, 10 -> Julia], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11, Kalle, 11 -> Kalle], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11, Kalle, 11 -> Kalle], after=[11, Kella, 11 -> Kella], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12, Lynx, 12 -> Lynx], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12, Lynx, 12 -> Lynx], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[13, Munroe, 13 -> Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[14, Neko, 14 -> Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[14, Neko, 14 -> Neko], after=[14, Nein, 14 -> Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[15, Oops, 15 -> Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[15, Oops, 15 -> Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testPreAsteriskWithSchemaEvolution() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        tableId.toString(),
                                        "*, CAST(id AS VARCHAR) || ' -> ' || name AS extend_id",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`extend_id` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 21, 1 -> Alice], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22, 2 -> Barcarolle], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23, 3 -> Cecily], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23, 3 -> Cecily], after=[3, Colin, 24, 3 -> Colin], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22, 2 -> Barcarolle], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0, 4 -> Derrida], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1, 5 -> Eve], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1, 5 -> Eve], after=[2nd, 5, Eva, 20, 2, 5 -> Eva], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3, 6 -> Fiona], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3, 6 -> Fiona], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1, 7 -> Gem], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2, 8 -> Helen], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2, 8 -> Helen], after=[5th, 8, Harry, 18.0, -3, 8 -> Harry], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0, 9 -> IINA], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0, 9 -> IINA], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1, 10 -> Julia], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0, 11 -> Kalle], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0, 11 -> Kalle], after=[8th, 11, Kella, 18.0, 0, 11 -> Kella], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0, 12 -> Lynx], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0, 12 -> Lynx], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe, 13 -> Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko, 14 -> Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko, 14 -> Neko], after=[11th, 14, Nein, 14 -> Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops, 15 -> Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops, 15 -> Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testPostAsteriskWithSchemaEvolution() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        tableId.toString(),
                                        "CAST(id AS VARCHAR) || ' -> ' || name AS extend_id, *",
                                        "id > 0",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`extend_id` STRING,`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1 -> Alice, 1, Alice, 21], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2 -> Barcarolle, 2, Barcarolle, 22], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3 -> Cecily, 3, Cecily, 23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3 -> Cecily, 3, Cecily, 23], after=[3 -> Colin, 3, Colin, 24], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2 -> Barcarolle, 2, Barcarolle, 22], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4 -> Derrida, 1st, 4, Derrida, 24, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5 -> Eve, 2nd, 5, Eve, 25, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5 -> Eve, 2nd, 5, Eve, 25, 1], after=[5 -> Eva, 2nd, 5, Eva, 20, 2], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6 -> Fiona, 3rd, 6, Fiona, 26, 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6 -> Fiona, 3rd, 6, Fiona, 26, 3], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7 -> Gem, 4th, 7, Gem, 19.0, -1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8 -> Helen, 5th, 8, Helen, 18.0, -2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8 -> Helen, 5th, 8, Helen, 18.0, -2], after=[8 -> Harry, 5th, 8, Harry, 18.0, -3], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9 -> IINA, 6th, 9, IINA, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9 -> IINA, 6th, 9, IINA, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10 -> Julia, 7th, 10, Julia, 24.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11 -> Kalle, 8th, 11, Kalle, 23.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11 -> Kalle, 8th, 11, Kalle, 23.0, 0], after=[11 -> Kella, 8th, 11, Kella, 18.0, 0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12 -> Lynx, 9th, 12, Lynx, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12 -> Lynx, 9th, 12, Lynx, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[13 -> Munroe, 10th, 13, Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[14 -> Neko, 11th, 14, Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[14 -> Neko, 11th, 14, Neko], after=[14 -> Nein, 11th, 14, Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[15 -> Oops, 12th, 15, Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[15 -> Oops, 12th, 15, Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testTransformWithFilterButNoProjection() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();

        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "default_namespace.default_schema.\\.*",
                                        null,
                                        "id > 1",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23], after=[3, Colin, 24], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1], after=[2nd, 5, Eva, 20, 2], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2], after=[5th, 8, Harry, 18.0, -3], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0], after=[8th, 11, Kella, 18.0, 0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko], after=[11th, 14, Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testTransformUnmatchedSchemaEvolution() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();

        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "foo.bar.baz", // This doesn't match given tableId
                                        "*",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 21], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23], after=[3, Colin, 24], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1], after=[2nd, 5, Eva, 20, 2], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2], after=[5th, 8, Harry, 18.0, -3], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0], after=[8th, 11, Kella, 18.0, 0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko], after=[11th, 14, Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testExplicitPrimaryKeyWithNullable() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "default_namespace.default_schema.mytable1",
                                        null,
                                        null,
                                        "name",
                                        "id,name",
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=name, partitionKeys=id;name, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 21], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23], after=[3, Colin, 24], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22], after=[], op=DELETE, meta=()}",

                        // Add column stage
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1], after=[2nd, 5, Eva, 20, 2], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3], after=[], op=DELETE, meta=()}",

                        // Alter column type stage
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2], after=[5th, 8, Harry, 18.0, -3], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Rename column stage
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0], after=[8th, 11, Kella, 18.0, 0], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0], after=[], op=DELETE, meta=()}",

                        // Drop column stage
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko], after=[11th, 14, Nein], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testTransformWithCommentsAndDefaultExpr() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId myTable1 = TableId.tableId("default_namespace", "default_schema", "mytable1");
        TableId myTable2 = TableId.tableId("default_namespace", "default_schema", "mytable2");
        Schema table1Schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT(), "id column", "AUTO_INCREMENT()")
                        .physicalColumn("name", DataTypes.STRING(), "name column", "Jane Doe")
                        .physicalColumn("age", DataTypes.INT(), "age column", "17")
                        .partitionKey("id, age")
                        .primaryKey("id")
                        .build();
        Schema table2Schema =
                Schema.newBuilder()
                        .physicalColumn(
                                "id", DataTypes.BIGINT(), "column for id", "AUTO_DECREMENT()")
                        .physicalColumn(
                                "name", DataTypes.VARCHAR(255), "column for name", "John Smith")
                        .physicalColumn("age", DataTypes.TINYINT(), "column for age", "91")
                        .physicalColumn(
                                "description",
                                DataTypes.STRING(),
                                "column for descriptions",
                                "not important")
                        .primaryKey("id")
                        .partitionKey("id, name")
                        .build();

        List<Event> events = getTestEvents(table1Schema, table2Schema, myTable1, myTable2);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "default_namespace.default_schema.\\.*",
                                        "*, name AS new_name, age + 1 AS new_age, 'extras' AS extras",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        Assertions.assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL 'id column' 'AUTO_INCREMENT()',`name` STRING 'name column' 'Jane Doe',`age` INT 'age column' '17',`new_name` STRING 'name column' 'Jane Doe',`new_age` INT,`extras` STRING}, primaryKeys=id, partitionKeys=id, age, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, Alice, 19, extras], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, Bob, 21, extras], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, Bob, 21, extras], after=[2, Bob, 30, Bob, 31, extras], op=UPDATE, meta=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL 'column for id' 'AUTO_DECREMENT()',`name` VARCHAR(255) 'column for name' 'John Smith',`age` TINYINT 'column for age' '91',`description` STRING 'column for descriptions' 'not important',`new_name` VARCHAR(255) 'column for name' 'John Smith',`new_age` INT,`extras` STRING}, primaryKeys=id, partitionKeys=id, name, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, Carol, 16, extras], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, Derrida, 26, extras], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, Derrida, 26, extras], after=[], op=DELETE, meta=()}");
    }

    String[] runNumericCastingWith(String expression) throws Exception {
        try {
            FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

            // Setup value source
            Configuration sourceConfig = new Configuration();

            sourceConfig.set(
                    ValuesDataSourceOptions.EVENT_SET_ID,
                    ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

            TableId tableId = TableId.tableId("ns", "scm", "tbl");
            List<Event> events = generateNumericCastingEvents(tableId);

            ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

            SourceDef sourceDef =
                    new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

            // Setup value sink
            Configuration sinkConfig = new Configuration();
            SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

            // Setup pipeline
            Configuration pipelineConfig = new Configuration();
            pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
            pipelineConfig.set(
                    PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
            PipelineDef pipelineDef =
                    new PipelineDef(
                            sourceDef,
                            sinkDef,
                            Collections.emptyList(),
                            Collections.singletonList(
                                    new TransformDef(
                                            "ns.scm.tbl",
                                            expression,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null)),
                            Collections.emptyList(),
                            pipelineConfig);

            // Execute the pipeline
            PipelineExecution execution = composer.compose(pipelineDef);
            execution.execute();

            // Check the order and content of all received events
            return outCaptor.toString().trim().split("\n");
        } finally {
            outCaptor.reset();
        }
    }

    // Generate a projection expression like CAST(tiny AS <T>) AS tiny, ...
    private static String generateCastTo(String type) {
        return "id, "
                + Stream.of(
                                "tiny",
                                "small",
                                "int",
                                "bigint",
                                "float",
                                "double",
                                "decimal",
                                "valid_char",
                                "invalid_char")
                        .map(col -> String.format("CAST(%s_c AS %s) AS %s_c", col, type, col))
                        .collect(Collectors.joining(", "));
    }

    @Test
    void testNumericCastingsWithTruncation() throws Exception {
        assertThat(runNumericCastingWith("*"))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` TINYINT,`small_c` SMALLINT,`int_c` INT,`bigint_c` BIGINT,`float_c` FLOAT,`double_c` DOUBLE,`decimal_c` DECIMAL(10, 2),`valid_char_c` VARCHAR(17),`invalid_char_c` VARCHAR(17)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -6.7, -8.9, -10.11, -12.13, foo], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0.0, 0.0, 0.00, 0, bar], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 6.7, 8.9, 10.11, 12.13, baz], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("BOOLEAN")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` BOOLEAN,`small_c` BOOLEAN,`int_c` BOOLEAN,`bigint_c` BOOLEAN,`float_c` BOOLEAN,`double_c` BOOLEAN,`decimal_c` BOOLEAN,`valid_char_c` BOOLEAN,`invalid_char_c` BOOLEAN}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, true, true, true, true, true, true, true, false, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, false, false, false, false, false, false, false, false, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, true, true, true, true, true, true, true, false, false], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("TINYINT")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` TINYINT,`small_c` TINYINT,`int_c` TINYINT,`bigint_c` TINYINT,`float_c` TINYINT,`double_c` TINYINT,`decimal_c` TINYINT,`valid_char_c` TINYINT,`invalid_char_c` TINYINT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -6, -8, -10, -12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0, 0, 0, 0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 6, 8, 10, 12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("SMALLINT")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` SMALLINT,`small_c` SMALLINT,`int_c` SMALLINT,`bigint_c` SMALLINT,`float_c` SMALLINT,`double_c` SMALLINT,`decimal_c` SMALLINT,`valid_char_c` SMALLINT,`invalid_char_c` SMALLINT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -6, -8, -10, -12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0, 0, 0, 0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 6, 8, 10, 12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("INT")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` INT,`small_c` INT,`int_c` INT,`bigint_c` INT,`float_c` INT,`double_c` INT,`decimal_c` INT,`valid_char_c` INT,`invalid_char_c` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -6, -8, -10, -12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0, 0, 0, 0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 6, 8, 10, 12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("BIGINT")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` BIGINT,`small_c` BIGINT,`int_c` BIGINT,`bigint_c` BIGINT,`float_c` BIGINT,`double_c` BIGINT,`decimal_c` BIGINT,`valid_char_c` BIGINT,`invalid_char_c` BIGINT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -6, -8, -10, -12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0, 0, 0, 0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 6, 8, 10, 12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("FLOAT")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` FLOAT,`small_c` FLOAT,`int_c` FLOAT,`bigint_c` FLOAT,`float_c` FLOAT,`double_c` FLOAT,`decimal_c` FLOAT,`valid_char_c` FLOAT,`invalid_char_c` FLOAT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2.0, -3.0, -4.0, -5.0, -6.7, -8.9, -10.11, -12.13, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2.0, 3.0, 4.0, 5.0, 6.7, 8.9, 10.11, 12.13, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("DOUBLE")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` DOUBLE,`small_c` DOUBLE,`int_c` DOUBLE,`bigint_c` DOUBLE,`float_c` DOUBLE,`double_c` DOUBLE,`decimal_c` DOUBLE,`valid_char_c` DOUBLE,`invalid_char_c` DOUBLE}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2.0, -3.0, -4.0, -5.0, -6.699999809265137, -8.9, -10.11, -12.13, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2.0, 3.0, 4.0, 5.0, 6.699999809265137, 8.9, 10.11, 12.13, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("DECIMAL(1, 0)")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` DECIMAL(1, 0),`small_c` DECIMAL(1, 0),`int_c` DECIMAL(1, 0),`bigint_c` DECIMAL(1, 0),`float_c` DECIMAL(1, 0),`double_c` DECIMAL(1, 0),`decimal_c` DECIMAL(1, 0),`valid_char_c` DECIMAL(1, 0),`invalid_char_c` DECIMAL(1, 0)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -7, -9, null, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0, 0, 0, 0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 7, 9, null, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("DECIMAL(2, 0)")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` DECIMAL(2, 0),`small_c` DECIMAL(2, 0),`int_c` DECIMAL(2, 0),`bigint_c` DECIMAL(2, 0),`float_c` DECIMAL(2, 0),`double_c` DECIMAL(2, 0),`decimal_c` DECIMAL(2, 0),`valid_char_c` DECIMAL(2, 0),`invalid_char_c` DECIMAL(2, 0)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2, -3, -4, -5, -7, -9, -10, -12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0, 0, 0, 0, 0, 0, 0, 0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2, 3, 4, 5, 7, 9, 10, 12, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("DECIMAL(3, 1)")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` DECIMAL(3, 1),`small_c` DECIMAL(3, 1),`int_c` DECIMAL(3, 1),`bigint_c` DECIMAL(3, 1),`float_c` DECIMAL(3, 1),`double_c` DECIMAL(3, 1),`decimal_c` DECIMAL(3, 1),`valid_char_c` DECIMAL(3, 1),`invalid_char_c` DECIMAL(3, 1)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2.0, -3.0, -4.0, -5.0, -6.7, -8.9, -10.1, -12.1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2.0, 3.0, 4.0, 5.0, 6.7, 8.9, 10.1, 12.1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");

        assertThat(runNumericCastingWith(generateCastTo("DECIMAL(19, 10)")))
                .containsExactly(
                        "CreateTableEvent{tableId=ns.scm.tbl, schema=columns={`id` BIGINT NOT NULL,`tiny_c` DECIMAL(19, 10),`small_c` DECIMAL(19, 10),`int_c` DECIMAL(19, 10),`bigint_c` DECIMAL(19, 10),`float_c` DECIMAL(19, 10),`double_c` DECIMAL(19, 10),`decimal_c` DECIMAL(19, 10),`valid_char_c` DECIMAL(19, 10),`invalid_char_c` DECIMAL(19, 10)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[-1, -2.0000000000, -3.0000000000, -4.0000000000, -5.0000000000, -6.7000000000, -8.9000000000, -10.1100000000, -12.1300000000, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[0, 0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000, 0.0000000000, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[1, 2.0000000000, 3.0000000000, 4.0000000000, 5.0000000000, 6.7000000000, 8.9000000000, 10.1100000000, 12.1300000000, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=ns.scm.tbl, before=[], after=[2, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testTransformWithLargeLiterals() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();

        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateSchemaEvolutionEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "\\.*.\\.*.\\.*",
                                        "*, 2147483647 AS int_max, "
                                                + "2147483648 AS greater_than_int_max, "
                                                + "-2147483648 AS int_min, "
                                                + "-2147483649 AS less_than_int_min, "
                                                + "CAST(1234567890123456789 AS DECIMAL(19, 0)) AS really_big_decimal",
                                        "CAST(id AS BIGINT) + 2147483648 > 2147483649", // equivalent to id > 1
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        // Initial stage
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`int_max` INT,`greater_than_int_max` BIGINT,`int_min` INT,`less_than_int_min` BIGINT,`really_big_decimal` DECIMAL(19, 0)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Barcarolle, 22, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3, Cecily, 23, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3, Cecily, 23, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[3, Colin, 24, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Barcarolle, 22, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[], op=DELETE, meta=()}",

                        // Add Column
                        "AddColumnEvent{tableId=default_namespace.default_schema.mytable1, addedColumns=[ColumnWithPosition{column=`rank` STRING, position=BEFORE, existedColumnName=id}, ColumnWithPosition{column=`gender` TINYINT, position=AFTER, existedColumnName=age}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1st, 4, Derrida, 24, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2nd, 5, Eve, 25, 1, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2nd, 5, Eve, 25, 1, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[2nd, 5, Eva, 20, 2, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[3rd, 6, Fiona, 26, 3, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[3rd, 6, Fiona, 26, 3, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[], op=DELETE, meta=()}",

                        // Alter column type
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.mytable1, typeMapping={gender=INT, name=VARCHAR(17), age=DOUBLE}, oldTypeMapping={gender=TINYINT, name=STRING, age=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4th, 7, Gem, 19.0, -1, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5th, 8, Helen, 18.0, -2, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[5th, 8, Helen, 18.0, -2, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[5th, 8, Harry, 18.0, -3, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[6th, 9, IINA, 17.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[6th, 9, IINA, 17.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[], op=DELETE, meta=()}",

                        // Rename column
                        "RenameColumnEvent{tableId=default_namespace.default_schema.mytable1, nameMapping={gender=biological_sex, age=toshi}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[7th, 10, Julia, 24.0, 1, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[8th, 11, Kalle, 23.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[8th, 11, Kalle, 23.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[8th, 11, Kella, 18.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9th, 12, Lynx, 17.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[9th, 12, Lynx, 17.0, 0, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[], op=DELETE, meta=()}",

                        // Drop column
                        "DropColumnEvent{tableId=default_namespace.default_schema.mytable1, droppedColumnNames=[biological_sex, toshi]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[10th, 13, Munroe, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[11th, 14, Neko, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[11th, 14, Neko, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[11th, 14, Nein, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[12th, 15, Oops, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[12th, 15, Oops, 2147483647, 2147483648, -2147483648, -2147483649, 1234567890123456789], after=[], op=DELETE, meta=()}");
    }

    @Test
    void testFloorCeilAndRoundFunction() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();

        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateFloorCeilAndRoundEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "\\.*.\\.*.\\.*",
                                        "*, "
                                                + "CEIL(tinyint_col)   AS ceil_tinyint,"
                                                + "CEIL(smallint_col)  AS ceil_smallint,"
                                                + "CEIL(int_col)       AS ceil_int,"
                                                + "CEIL(bigint_col)    AS ceil_bigint,"
                                                + "CEIL(float_col)     AS ceil_float,"
                                                + "CEIL(double_col)    AS ceil_double,"
                                                + "CEIL(decimal_col)   AS ceil_decimal,"
                                                + "CEILING(tinyint_col)   AS ceiling_tinyint,"
                                                + "CEILING(smallint_col)  AS ceiling_smallint,"
                                                + "CEILING(int_col)       AS ceiling_int,"
                                                + "CEILING(bigint_col)    AS ceiling_bigint,"
                                                + "CEILING(float_col)     AS ceiling_float,"
                                                + "CEILING(double_col)    AS ceiling_double,"
                                                + "CEILING(decimal_col)   AS ceiling_decimal,"
                                                + "FLOOR(tinyint_col)  AS floor_tinyint,"
                                                + "FLOOR(smallint_col) AS floor_smallint,"
                                                + "FLOOR(int_col)      AS floor_int,"
                                                + "FLOOR(bigint_col)   AS floor_bigint,"
                                                + "FLOOR(float_col)    AS floor_float,"
                                                + "FLOOR(double_col)   AS floor_double,"
                                                + "FLOOR(decimal_col)  AS floor_decimal,"
                                                + "ROUND(tinyint_col, 2)  AS round_tinyint,"
                                                + "ROUND(smallint_col, 2) AS round_smallint,"
                                                + "ROUND(int_col, 2)      AS round_int,"
                                                + "ROUND(bigint_col, 2)   AS round_bigint,"
                                                + "ROUND(float_col, 2)    AS round_float,"
                                                + "ROUND(double_col, 2)   AS round_double,"
                                                + "ROUND(decimal_col, 2)  AS round_decimal,"
                                                + "ROUND(tinyint_col, 0)  AS round_0_tinyint,"
                                                + "ROUND(smallint_col, 0) AS round_0_smallint,"
                                                + "ROUND(int_col, 0)      AS round_0_int,"
                                                + "ROUND(bigint_col, 0)   AS round_0_bigint,"
                                                + "ROUND(float_col, 0)    AS round_0_float,"
                                                + "ROUND(double_col, 0)   AS round_0_double,"
                                                + "ROUND(decimal_col, 0)  AS round_0_decimal",
                                        null,
                                        "id",
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`tinyint_col` TINYINT,`smallint_col` SMALLINT,`int_col` INT,`bigint_col` BIGINT,`float_col` FLOAT,`double_col` DOUBLE,`decimal_col` DECIMAL(10, 3),`ceil_tinyint` TINYINT,`ceil_smallint` SMALLINT,`ceil_int` INT,`ceil_bigint` BIGINT,`ceil_float` FLOAT,`ceil_double` DOUBLE,`ceil_decimal` DECIMAL(10, 0),`ceiling_tinyint` TINYINT,`ceiling_smallint` SMALLINT,`ceiling_int` INT,`ceiling_bigint` BIGINT,`ceiling_float` FLOAT,`ceiling_double` DOUBLE,`ceiling_decimal` DECIMAL(10, 0),`floor_tinyint` TINYINT,`floor_smallint` SMALLINT,`floor_int` INT,`floor_bigint` BIGINT,`floor_float` FLOAT,`floor_double` DOUBLE,`floor_decimal` DECIMAL(10, 0),`round_tinyint` TINYINT,`round_smallint` SMALLINT,`round_int` INT,`round_bigint` BIGINT,`round_float` FLOAT,`round_double` DOUBLE,`round_decimal` DECIMAL(10, 2),`round_0_tinyint` TINYINT,`round_0_smallint` SMALLINT,`round_0_int` INT,`round_0_bigint` BIGINT,`round_0_float` FLOAT,`round_0_double` DOUBLE,`round_0_decimal` DECIMAL(8, 0)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, 1, 1, 1, 1, 1.1, 1.1, 1.100, 1, 1, 1, 1, 2.0, 2.0, 2, 1, 1, 1, 1, 2.0, 2.0, 2, 1, 1, 1, 1, 1.0, 1.0, 1, 1, 1, 1, 1, 1.1, 1.1, 1.10, 1, 1, 1, 1, 1.0, 1.0, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[4, 4, 4, 4, 4, 4.44, 4.44, 4.440, 4, 4, 4, 4, 5.0, 5.0, 5, 4, 4, 4, 4, 5.0, 5.0, 5, 4, 4, 4, 4, 4.0, 4.0, 4, 4, 4, 4, 4, 4.44, 4.44, 4.44, 4, 4, 4, 4, 4.0, 4.0, 4], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[5, 5, 5, 5, 5, 5.555, 5.555, 5.555, 5, 5, 5, 5, 6.0, 6.0, 6, 5, 5, 5, 5, 6.0, 6.0, 6, 5, 5, 5, 5, 5.0, 5.0, 5, 5, 5, 5, 5, 5.56, 5.56, 5.56, 5, 5, 5, 5, 6.0, 6.0, 6], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[9, 9, 9, 9, 9, 1.0E7, 9999999.999, 9999999.999, 9, 9, 9, 9, 1.0E7, 1.0E7, 10000000, 9, 9, 9, 9, 1.0E7, 1.0E7, 10000000, 9, 9, 9, 9, 1.0E7, 9999999.0, 9999999, 9, 9, 9, 9, 1.0E7, 1.0E7, 10000000.00, 9, 9, 9, 9, 1.0E7, 1.0E7, 10000000], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testAbsFunction() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();

        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        List<Event> events = generateAbsEvents(tableId);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "\\.*.\\.*.\\.*",
                                        "*, "
                                                + "ABS(tinyint_col)   AS abs_tinyint,"
                                                + "ABS(smallint_col)  AS abs_smallint,"
                                                + "ABS(int_col)       AS abs_int,"
                                                + "ABS(bigint_col)    AS abs_bigint,"
                                                + "ABS(float_col)     AS abs_float,"
                                                + "ABS(double_col)    AS abs_double,"
                                                + "ABS(decimal_col)   AS abs_decimal",
                                        null,
                                        "id",
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`tinyint_col` TINYINT,`smallint_col` SMALLINT,`int_col` INT,`bigint_col` BIGINT,`float_col` FLOAT,`double_col` DOUBLE,`decimal_col` DECIMAL(10, 2),`abs_tinyint` TINYINT,`abs_smallint` SMALLINT,`abs_int` INT,`abs_bigint` BIGINT,`abs_float` FLOAT,`abs_double` DOUBLE,`abs_decimal` DECIMAL(10, 2)}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, 1, 1, 1, 1, 1.1, 1.1, 1.10, 1, 1, 1, 1, 1.1, 1.1, 1.10], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[-4, -4, -4, -4, -4, -4.44, -4.44, -4.44, 4, 4, 4, 4, 4.44, 4.44, 4.44], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[-9, -9, -9, -9, -9, -1.0E8, -9.999999999E7, -99999999.99, 9, 9, 9, 9, 1.0E8, 9.999999999E7, 99999999.99], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[0, null, null, null, null, null, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");
    }

    @Test
    void testTransformErrorMessage() {
        // Unexpected column in projection rule
        assertThatThrownBy(expectTransformError("id1", null, "id"))
                .cause()
                .cause()
                .cause()
                .isExactlyInstanceOf(TransformException.class)
                .hasStackTraceContaining(
                        "Failed to post-transform with\n"
                                + "\tCreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={}, primaryKeys=id, options=()}\n"
                                + "for table\n"
                                + "\tdefault_namespace.default_schema.mytable1\n"
                                + "from schema\n"
                                + "\t(Unknown)\n"
                                + "to schema\n"
                                + "\t(Unknown).")
                .rootCause()
                .isExactlyInstanceOf(SqlValidatorException.class)
                .hasMessage("Column 'id1' not found in any table");

        // Unexpected column in filter rule
        assertThatThrownBy(expectTransformError("*", "id1 > 0", "id"))
                .cause()
                .cause()
                .cause()
                .isExactlyInstanceOf(TransformException.class)
                .hasMessage(
                        "Failed to post-transform with\n"
                                + "\tDataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=null, after={id: INT -> 1, name: STRING -> Alice, age: INT -> 18}, op=INSERT, meta=()}\n"
                                + "for table\n"
                                + "\tdefault_namespace.default_schema.mytable1\n"
                                + "from schema\n"
                                + "\tcolumns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()\n"
                                + "to schema\n"
                                + "\tcolumns={`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=().")
                .cause()
                .isExactlyInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "Failed to compile expression TransformExpressionKey{expression='import static org.apache.flink.cdc.runtime.functions.SystemFunctionUtils.*;greaterThan($0, 0)', argumentNames=[__time_zone__, __epoch_time__], argumentClasses=[class java.lang.String, class java.lang.Long], returnClass=class java.lang.Boolean, columnNameMap={id1=$0}}")
                .cause()
                .hasMessageContaining(
                        "Expression: import static org.apache.flink.cdc.runtime.functions.SystemFunctionUtils.*;greaterThan($0, 0)")
                .hasMessageContaining("Column name map: {$0 -> id1}")
                .rootCause()
                .isExactlyInstanceOf(CompileException.class)
                .hasMessageContaining("Unknown variable or type \"$0\"");

        // Unexpected column in filter rule
        Assertions.assertThatThrownBy(expectTransformError("name", null, "id"))
                .cause()
                .cause()
                .cause()
                .isExactlyInstanceOf(TransformException.class)
                .hasMessage(
                        "Failed to post-transform with\n"
                                + "\tCreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`name` STRING}, primaryKeys=id, options=()}\n"
                                + "for table\n"
                                + "\tdefault_namespace.default_schema.mytable1\n"
                                + "from schema\n"
                                + "\tcolumns={`name` STRING}, primaryKeys=id, options=()\n"
                                + "to schema\n"
                                + "\tcolumns={`name` STRING}, primaryKeys=id, options=().")
                .rootCause()
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Unable to find column \"id\" which is defined as primary key");

        // Unsupported operations in projection rule
        Assertions.assertThatThrownBy(expectTransformError("id, name + 1 AS new_name", null, "id"))
                .cause()
                .cause()
                .cause()
                .isExactlyInstanceOf(TransformException.class)
                .hasMessage(
                        "Failed to post-transform with\n"
                                + "\tDataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=null, after={id: INT -> 1, name: STRING -> Alice}, op=INSERT, meta=()}\n"
                                + "for table\n"
                                + "\tdefault_namespace.default_schema.mytable1\n"
                                + "from schema\n"
                                + "\tcolumns={`id` INT,`name` STRING}, primaryKeys=id, options=()\n"
                                + "to schema\n"
                                + "\tcolumns={`id` INT NOT NULL,`new_name` INT}, primaryKeys=id, options=().")
                .cause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Failed to evaluate projection expression `castToInteger($0) + 1` for column `new_name` in table `default_namespace.default_schema.mytable1`")
                .hasMessageContaining("Column name map: {$0 -> name}");

        // Unsupported operations in filter rule
        assertThatThrownBy(expectTransformError("*", "name + 1 > 0", "id"))
                .cause()
                .cause()
                .cause()
                .isExactlyInstanceOf(TransformException.class)
                .hasMessage(
                        "Failed to post-transform with\n"
                                + "\tDataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=null, after={id: INT -> 1, name: STRING -> Alice, age: INT -> 18}, op=INSERT, meta=()}\n"
                                + "for table\n"
                                + "\tdefault_namespace.default_schema.mytable1\n"
                                + "from schema\n"
                                + "\tcolumns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()\n"
                                + "to schema\n"
                                + "\tcolumns={`id` INT NOT NULL,`name` STRING,`age` INT}, primaryKeys=id, options=().")
                .cause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Failed to evaluate filtering expression `greaterThan($0 + 1, 0)` for table `default_namespace.default_schema.mytable1`")
                .hasMessageContaining("Column name map: {$0 -> name}")
                .rootCause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage(
                        "Comparison of unsupported data types: java.lang.String and java.lang.Integer");

        // Unsupported operations in metadata rule
        Assertions.assertThatThrownBy(expectTransformError("*", null, "not_even_exist"))
                .cause()
                .cause()
                .cause()
                .isExactlyInstanceOf(TransformException.class)
                .hasMessage(
                        "Failed to post-transform with\n"
                                + "\tCreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=not_even_exist, options=()}\n"
                                + "for table\n"
                                + "\tdefault_namespace.default_schema.mytable1\n"
                                + "from schema\n"
                                + "\tcolumns={`id` INT,`name` STRING,`age` INT}, primaryKeys=not_even_exist, options=()\n"
                                + "to schema\n"
                                + "\tcolumns={`id` INT,`name` STRING,`age` INT}, primaryKeys=not_even_exist, options=().")
                .rootCause()
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Unable to find column \"not_even_exist\" which is defined as primary key");
    }

    private ThrowableAssert.ThrowingCallable expectTransformError(
            String projectionRule, String filterRule, String primaryKeys) {
        return () ->
                runGenericTransformTest(
                        ValuesDataSink.SinkApi.SINK_V2,
                        Collections.singletonList(
                                new TransformDef(
                                        "default_namespace.default_schema.\\.*",
                                        projectionRule,
                                        filterRule,
                                        primaryKeys,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList());
    }

    @Test
    void testShadeOriginalColumnsWithDifferentType() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.TRANSFORM_TABLE);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);
        Configuration sinkConfig = new Configuration();
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        "\\.*.\\.*.\\.*",
                                        "*, 0.5 + CAST(col1 AS DOUBLE) AS col1",
                                        "col1 > 1.5",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null)),
                        Collections.emptyList(),
                        pipelineConfig);
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` DOUBLE NOT NULL,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2.5, 2], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3.5, 3], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2.5, ], after=[2.5, x], op=UPDATE, meta=({op_ts=5})}");
    }

    private static final String[] UNICODE_STRINGS = {
        "ascii test!?",
        "大五",
        "测试数据",
        "ひびぴ",
        "죠주쥬",
        "ÀÆÉ",
        "ÓÔŐÖ",
        "αβγδε",
        "בבקשה",
        "твой",
        "ภาษาไทย",
        "piedzimst brīvi"
    };

    @ParameterizedTest
    @EnumSource
    void testTransformProjectionWithUnicodeCharacters(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        for (String unicodeString : UNICODE_STRINGS) {
            List<String> expected =
                    Stream.of(
                                    "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`prefix` STRING,`id` INT NOT NULL,`name` STRING,`age` INT,`suffix` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[{UNICODE_STRING} -> 1, 1, Alice, 18, 1 <- {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[{UNICODE_STRING} -> 2, 2, Bob, 20, 2 <- {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[{UNICODE_STRING} -> 2, 2, Bob, 20, 2 <- {UNICODE_STRING}], after=[{UNICODE_STRING} -> 2, 2, Bob, 30, 2 <- {UNICODE_STRING}], op=UPDATE, meta=()}",
                                    "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`prefix` STRING,`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`suffix` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[{UNICODE_STRING} -> 3, 3, Carol, 15, student, 3 <- {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[{UNICODE_STRING} -> 4, 4, Derrida, 25, student, 4 <- {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[{UNICODE_STRING} -> 4, 4, Derrida, 25, student, 4 <- {UNICODE_STRING}], after=[], op=DELETE, meta=()}")
                            .map(tpl -> tpl.replace("{UNICODE_STRING}", unicodeString))
                            .collect(Collectors.toList());

            runGenericTransformTest(
                    sinkApi,
                    Collections.singletonList(
                            new TransformDef(
                                    "\\.*.\\.*.\\.*",
                                    String.format(
                                            "'%s' || ' -> ' || id AS prefix, *, id || ' <- ' || '%s' AS suffix",
                                            unicodeString, unicodeString),
                                    null,
                                    null,
                                    "id",
                                    null,
                                    null,
                                    null)),
                    expected);
            outCaptor.reset();
        }
    }

    @ParameterizedTest
    @EnumSource
    void testTransformFilterWithUnicodeCharacters(ValuesDataSink.SinkApi sinkApi) throws Exception {
        for (String unicodeString : UNICODE_STRINGS) {
            List<String> expected =
                    Stream.of(
                                    "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`extras` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, Alice, 18, {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[2, Bob, 20, {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[2, Bob, 20, {UNICODE_STRING}], after=[2, Bob, 30, {UNICODE_STRING}], op=UPDATE, meta=()}",
                                    "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`extras` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[3, Carol, 15, student, {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[], after=[4, Derrida, 25, student, {UNICODE_STRING}], op=INSERT, meta=()}",
                                    "DataChangeEvent{tableId=default_namespace.default_schema.mytable2, before=[4, Derrida, 25, student, {UNICODE_STRING}], after=[], op=DELETE, meta=()}")
                            .map(tpl -> tpl.replace("{UNICODE_STRING}", unicodeString))
                            .collect(Collectors.toList());

            runGenericTransformTest(
                    sinkApi,
                    Collections.singletonList(
                            new TransformDef(
                                    "\\.*.\\.*.\\.*",
                                    String.format("*, '%s' AS extras", unicodeString),
                                    String.format("extras = '%s'", unicodeString),
                                    null,
                                    "id",
                                    null,
                                    null,
                                    null)),
                    expected);
            outCaptor.reset();

            runGenericTransformTest(
                    sinkApi,
                    Collections.singletonList(
                            new TransformDef(
                                    "\\.*.\\.*.\\.*",
                                    String.format("*, '%s' AS extras", unicodeString),
                                    String.format("extras <> '%s'", unicodeString),
                                    null,
                                    "id",
                                    null,
                                    null,
                                    null)),
                    Arrays.asList(
                            "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`extras` STRING}, primaryKeys=id, partitionKeys=id, options=()}",
                            "CreateTableEvent{tableId=default_namespace.default_schema.mytable2, schema=columns={`id` BIGINT NOT NULL,`name` VARCHAR(255),`age` TINYINT,`description` STRING,`extras` STRING}, primaryKeys=id, partitionKeys=id, options=()}"));
            outCaptor.reset();
        }
    }

    @Test
    void testDateAndTimeCastingFunctions() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        TableId tableId = TableId.tableId("default_namespace", "default_schema", "my_table");
        Schema tableSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("date_0", DataTypes.DATE())
                        .physicalColumn("time_0", DataTypes.TIME(0))
                        .physicalColumn("time_3", DataTypes.TIME(3))
                        .physicalColumn("time_6", DataTypes.TIME(6))
                        .physicalColumn("time_9", DataTypes.TIME(9))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        tableSchema.getColumnDataTypes().toArray(new DataType[0]));

        List<Event> events =
                Arrays.asList(
                        new CreateTableEvent(tableId, tableSchema),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {
                                            1,
                                            DateData.fromLocalDate(LocalDate.of(1999, 12, 31)),
                                            TimeData.fromLocalTime(LocalTime.of(21, 48, 25)),
                                            TimeData.fromLocalTime(
                                                    LocalTime.of(21, 48, 25, 123000000)),
                                            TimeData.fromLocalTime(
                                                    LocalTime.of(21, 48, 25, 123456000)),
                                            TimeData.fromLocalTime(
                                                    LocalTime.of(21, 48, 25, 123456789))
                                        })),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generator.generate(
                                        new Object[] {2, null, null, null, null, null})));

        TransformDef transformDef =
                new TransformDef(
                        tableId.toString(),
                        "*,"
                                + "CAST(date_0 AS VARCHAR) AS date_0_str,"
                                + "CAST(time_0 AS VARCHAR) AS time_0_str,"
                                + "CAST(time_3 AS VARCHAR) AS time_3_str,"
                                + "CAST(time_6 AS VARCHAR) AS time_6_str,"
                                + "CAST(time_9 AS VARCHAR) AS time_9_str",
                        null,
                        "id",
                        null,
                        null,
                        null,
                        null);

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, ValuesDataSink.SinkApi.SINK_V2);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(transformDef),
                        Collections.emptyList(),
                        pipelineConfig);

        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        assertThat(outputEvents)
                .containsExactlyInAnyOrder(
                        "CreateTableEvent{tableId=default_namespace.default_schema.my_table, schema=columns={`id` INT NOT NULL,`date_0` DATE,`time_0` TIME(0),`time_3` TIME(3),`time_6` TIME(6),`time_9` TIME(9),`date_0_str` STRING,`time_0_str` STRING,`time_3_str` STRING,`time_6_str` STRING,`time_9_str` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.my_table, before=[], after=[1, 1999-12-31, 21:48:25, 21:48:25.123, 21:48:25.123, 21:48:25.123, 1999-12-31, 21:48:25, 21:48:25.123, 21:48:25.123, 21:48:25.123], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.my_table, before=[], after=[2, null, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}");
    }

    private List<Event> generateFloorCeilAndRoundEvents(TableId tableId) {
        List<Event> events = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("tinyint_col", DataTypes.TINYINT())
                        .physicalColumn("smallint_col", DataTypes.SMALLINT())
                        .physicalColumn("int_col", DataTypes.INT())
                        .physicalColumn("bigint_col", DataTypes.BIGINT())
                        .physicalColumn("float_col", DataTypes.FLOAT())
                        .physicalColumn("double_col", DataTypes.DOUBLE())
                        .physicalColumn("decimal_col", DataTypes.DECIMAL(10, 3))
                        .primaryKey("id")
                        .build();

        events.add(new CreateTableEvent(tableId, schema));

        Stream.of(
                        generate(
                                schema,
                                1,
                                (byte) 1,
                                (short) 1,
                                1,
                                1L,
                                1.1f,
                                1.1d,
                                DecimalData.fromBigDecimal(new BigDecimal("1.1"), 10, 3)),
                        generate(
                                schema,
                                4,
                                (byte) 4,
                                (short) 4,
                                4,
                                4L,
                                4.44f,
                                4.44d,
                                DecimalData.fromBigDecimal(new BigDecimal("4.44"), 10, 3)),
                        generate(
                                schema,
                                5,
                                (byte) 5,
                                (short) 5,
                                5,
                                5L,
                                5.555f,
                                5.555d,
                                DecimalData.fromBigDecimal(new BigDecimal("5.555"), 10, 3)),
                        generate(
                                schema,
                                9,
                                (byte) 9,
                                (short) 9,
                                9,
                                9L,
                                9999999.999f,
                                9999999.999d,
                                DecimalData.fromBigDecimal(new BigDecimal("9999999.999"), 10, 3)),
                        generate(schema, 0, null, null, null, null, null, null, null))
                .map(rec -> DataChangeEvent.insertEvent(tableId, rec))
                .forEach(events::add);
        return events;
    }

    private List<Event> generateAbsEvents(TableId tableId) {
        List<Event> events = new ArrayList<>();
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("tinyint_col", DataTypes.TINYINT())
                        .physicalColumn("smallint_col", DataTypes.SMALLINT())
                        .physicalColumn("int_col", DataTypes.INT())
                        .physicalColumn("bigint_col", DataTypes.BIGINT())
                        .physicalColumn("float_col", DataTypes.FLOAT())
                        .physicalColumn("double_col", DataTypes.DOUBLE())
                        .physicalColumn("decimal_col", DataTypes.DECIMAL(10, 2))
                        .primaryKey("id")
                        .build();

        events.add(new CreateTableEvent(tableId, schema));

        Stream.of(
                        generate(
                                schema,
                                1,
                                (byte) 1,
                                (short) 1,
                                1,
                                1L,
                                1.1f,
                                1.1d,
                                DecimalData.fromBigDecimal(new BigDecimal("1.1"), 10, 2)),
                        generate(
                                schema,
                                -4,
                                (byte) -4,
                                (short) -4,
                                -4,
                                -4L,
                                -4.44f,
                                -4.44d,
                                DecimalData.fromBigDecimal(new BigDecimal("-4.44"), 10, 2)),
                        generate(
                                schema,
                                -9,
                                (byte) -9,
                                (short) -9,
                                -9,
                                -9L,
                                -99999999.99f,
                                -99999999.99d,
                                DecimalData.fromBigDecimal(new BigDecimal("-99999999.99"), 10, 2)),
                        generate(schema, 0, null, null, null, null, null, null, null))
                .map(rec -> DataChangeEvent.insertEvent(tableId, rec))
                .forEach(events::add);
        return events;
    }

    private List<Event> generateSchemaEvolutionEvents(TableId tableId) {
        List<Event> events = new ArrayList<>();

        // Initial schema
        {
            Schema schemaV1 =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .physicalColumn("age", DataTypes.INT())
                            .primaryKey("id")
                            .build();

            events.add(new CreateTableEvent(tableId, schemaV1));
            events.add(DataChangeEvent.insertEvent(tableId, generate(schemaV1, 1, "Alice", 21)));
            events.add(
                    DataChangeEvent.insertEvent(tableId, generate(schemaV1, 2, "Barcarolle", 22)));
            events.add(DataChangeEvent.insertEvent(tableId, generate(schemaV1, 3, "Cecily", 23)));
            events.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generate(schemaV1, 3, "Cecily", 23),
                            generate(schemaV1, 3, "Colin", 24)));
            events.add(
                    DataChangeEvent.deleteEvent(tableId, generate(schemaV1, 2, "Barcarolle", 22)));
        }

        // test AddColumnEvent
        {
            events.add(
                    new AddColumnEvent(
                            tableId,
                            Arrays.asList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("rank", DataTypes.STRING()),
                                            AddColumnEvent.ColumnPosition.FIRST,
                                            null),
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "gender", DataTypes.TINYINT())))));
            Schema schemaV2 =
                    Schema.newBuilder()
                            .physicalColumn("rank", DataTypes.STRING())
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .physicalColumn("age", DataTypes.INT())
                            .physicalColumn("gender", DataTypes.TINYINT())
                            .primaryKey("id")
                            .build();
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV2, "1st", 4, "Derrida", 24, (byte) 0)));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV2, "2nd", 5, "Eve", 25, (byte) 1)));
            events.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generate(schemaV2, "2nd", 5, "Eve", 25, (byte) 1),
                            generate(schemaV2, "2nd", 5, "Eva", 20, (byte) 2)));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV2, "3rd", 6, "Fiona", 26, (byte) 3)));
            events.add(
                    DataChangeEvent.deleteEvent(
                            tableId, generate(schemaV2, "3rd", 6, "Fiona", 26, (byte) 3)));
        }

        // test AlterColumnTypeEvent
        {
            events.add(
                    new AlterColumnTypeEvent(
                            tableId,
                            ImmutableMap.of(
                                    "age",
                                    DataTypes.DOUBLE(),
                                    "gender",
                                    DataTypes.INT(),
                                    "name",
                                    DataTypes.VARCHAR(17))));
            Schema schemaV3 =
                    Schema.newBuilder()
                            .physicalColumn("rank", DataTypes.STRING())
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .physicalColumn("age", DataTypes.DOUBLE())
                            .physicalColumn("gender", DataTypes.INT())
                            .primaryKey("id")
                            .build();
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV3, "4th", 7, "Gem", 19d, -1)));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV3, "5th", 8, "Helen", 18d, -2)));
            events.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generate(schemaV3, "5th", 8, "Helen", 18d, -2),
                            generate(schemaV3, "5th", 8, "Harry", 18d, -3)));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV3, "6th", 9, "IINA", 17d, 0)));
            events.add(
                    DataChangeEvent.deleteEvent(
                            tableId, generate(schemaV3, "6th", 9, "IINA", 17d, 0)));
        }

        // test RenameColumnEvent
        {
            events.add(
                    new RenameColumnEvent(
                            tableId, ImmutableMap.of("gender", "biological_sex", "age", "toshi")));
            Schema schemaV4 =
                    Schema.newBuilder()
                            .physicalColumn("rank", DataTypes.STRING())
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .physicalColumn("toshi", DataTypes.DOUBLE())
                            .physicalColumn("biological_sex", DataTypes.INT())
                            .primaryKey("id")
                            .build();
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV4, "7th", 10, "Julia", 24d, 1)));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV4, "8th", 11, "Kalle", 23d, 0)));
            events.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generate(schemaV4, "8th", 11, "Kalle", 23d, 0),
                            generate(schemaV4, "8th", 11, "Kella", 18d, 0)));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId, generate(schemaV4, "9th", 12, "Lynx", 17d, 0)));
            events.add(
                    DataChangeEvent.deleteEvent(
                            tableId, generate(schemaV4, "9th", 12, "Lynx", 17d, 0)));
        }

        // test DropColumnEvent
        {
            events.add(new DropColumnEvent(tableId, Arrays.asList("biological_sex", "toshi")));
            Schema schemaV5 =
                    Schema.newBuilder()
                            .physicalColumn("rank", DataTypes.STRING())
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .primaryKey("id")
                            .build();
            events.add(
                    DataChangeEvent.insertEvent(tableId, generate(schemaV5, "10th", 13, "Munroe")));
            events.add(
                    DataChangeEvent.insertEvent(tableId, generate(schemaV5, "11th", 14, "Neko")));
            events.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generate(schemaV5, "11th", 14, "Neko"),
                            generate(schemaV5, "11th", 14, "Nein")));
            events.add(
                    DataChangeEvent.insertEvent(tableId, generate(schemaV5, "12th", 15, "Oops")));
            events.add(
                    DataChangeEvent.deleteEvent(tableId, generate(schemaV5, "12th", 15, "Oops")));
        }
        return events;
    }

    private List<Event> generateNumericCastingEvents(TableId tableId) {
        List<Event> events = new ArrayList<>();

        // Initial schema
        {
            Schema schema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.BIGINT())
                            .physicalColumn("tiny_c", DataTypes.TINYINT())
                            .physicalColumn("small_c", DataTypes.SMALLINT())
                            .physicalColumn("int_c", DataTypes.INT())
                            .physicalColumn("bigint_c", DataTypes.BIGINT())
                            .physicalColumn("float_c", DataTypes.FLOAT())
                            .physicalColumn("double_c", DataTypes.DOUBLE())
                            .physicalColumn("decimal_c", DataTypes.DECIMAL(10, 2))
                            .physicalColumn("valid_char_c", DataTypes.VARCHAR(17))
                            .physicalColumn("invalid_char_c", DataTypes.VARCHAR(17))
                            .primaryKey("id")
                            .build();

            events.add(new CreateTableEvent(tableId, schema));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generate(
                                    schema,
                                    -1L,
                                    (byte) -2,
                                    (short) -3,
                                    -4,
                                    (long) -5,
                                    -6.7f,
                                    -8.9d,
                                    DecimalData.fromBigDecimal(new BigDecimal("-10.11"), 10, 2),
                                    "-12.13",
                                    "foo")));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generate(
                                    schema,
                                    0L,
                                    (byte) 0,
                                    (short) 0,
                                    0,
                                    (long) 0,
                                    0f,
                                    0d,
                                    DecimalData.fromBigDecimal(BigDecimal.ZERO, 10, 2),
                                    "0",
                                    "bar")));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generate(
                                    schema,
                                    1L,
                                    (byte) 2,
                                    (short) 3,
                                    4,
                                    (long) 5,
                                    6.7f,
                                    8.9d,
                                    DecimalData.fromBigDecimal(new BigDecimal("10.11"), 10, 2),
                                    "12.13",
                                    "baz")));
            events.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generate(
                                    schema, 2L, null, null, null, null, null, null, null, null,
                                    null)));
        }

        return events;
    }

    void extractDataLines(String line) {
        if (!line.startsWith("DataChangeEvent{")) {
            return;
        }
        Stream.of("before", "after")
                .forEach(
                        tag -> {
                            String[] arr = line.split(tag + "=\\[", 2);
                            String dataRecord = arr[arr.length - 1].split("]", 2)[0];
                            if (!dataRecord.isEmpty()) {
                                verifyDataRecord(dataRecord);
                            }
                        });
    }

    void verifyDataRecord(String recordLine) {
        List<String> tokens = Arrays.asList(recordLine.split(", "));
        assertThat(tokens).hasSizeGreaterThanOrEqualTo(6);

        tokens = tokens.subList(tokens.size() - 6, tokens.size());

        String localTime = tokens.get(0);
        String currentTime = tokens.get(1);
        assertThat(localTime).isEqualTo(currentTime);

        String currentTimestamp = tokens.get(2);
        String nowTimestamp = tokens.get(3);
        String localTimestamp = tokens.get(4);
        assertThat(currentTimestamp).isEqualTo(nowTimestamp).isEqualTo(localTimestamp);

        // If timestamp millisecond part is .000, it will be truncated to yyyy-MM-dd'T'HH:mm:ss
        // format. Manually append this for the following checks.
        if (currentTimestamp.length() == 19) {
            currentTimestamp += ".000";
        }

        Instant instant =
                LocalDateTime.parse(
                                currentTimestamp,
                                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))
                        .toInstant(ZoneOffset.UTC);

        long milliSecondsInOneDay = 24 * 60 * 60 * 1000;
        assertThat(TimeData.fromIsoLocalTimeString(localTime))
                .isEqualTo(
                        TimeData.fromMillisOfDay(
                                (int) (instant.toEpochMilli() % milliSecondsInOneDay)));

        String localDate = tokens.get(5);
        assertThat(DateData.fromIsoLocalDateString(localDate))
                .isEqualTo(
                        DateData.fromEpochDay(
                                (int) (instant.toEpochMilli() / milliSecondsInOneDay)));
    }

    BinaryRecordData generate(Schema schema, Object... fields) {
        return (new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }

    private Instant toInstant(String ts, String timezone) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of(timezone)).toInstant();
    }
}
