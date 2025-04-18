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
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
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
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_1;
import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_2;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link FlinkPipelineComposer}. */
class FlinkPipelineComposerITCase {

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

    @ParameterizedTest
    @EnumSource
    void testSingleSplitSingleTable(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_SINGLE_TABLE);
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

        // Check result in ValuesDatabase
        List<String> results = ValuesDatabase.getResults(TABLE_1);
        assertThat(results)
                .contains(
                        "default_namespace.default_schema.table1:col1=2;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;newCol3=");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, ], after=[2, x], op=UPDATE, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testSingleSplitMultipleTables(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
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

        // Check result in ValuesDatabase
        List<String> table1Results = ValuesDatabase.getResults(TABLE_1);
        assertThat(table1Results)
                .containsExactly(
                        "default_namespace.default_schema.table1:col1=2;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;newCol3=");
        List<String> table2Results = ValuesDatabase.getResults(TABLE_2);
        assertThat(table2Results)
                .contains(
                        "default_namespace.default_schema.table2:col1=1;col2=1",
                        "default_namespace.default_schema.table2:col1=2;col2=2",
                        "default_namespace.default_schema.table2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testMultiSplitsSingleTable(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.MULTI_SPLITS_SINGLE_TABLE);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, MAX_PARALLELISM);
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

        // Check result in ValuesDatabase
        List<String> table1Results = ValuesDatabase.getResults(TABLE_1);
        assertThat(table1Results)
                .contains(
                        "default_namespace.default_schema.table1:col1=1;col2=1;col3=x",
                        "default_namespace.default_schema.table1:col1=3;col2=3;col3=x",
                        "default_namespace.default_schema.table1:col1=5;col2=5;col3=");
    }

    @ParameterizedTest
    @EnumSource
    void testTransform(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.TRANSFORM_TABLE);
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
                        "*,concat(col1,'0') as col12",
                        "col1 <> '3'",
                        "col1",
                        "col12",
                        "key1=value1",
                        "",
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
                        new ArrayList<>(Collections.singletonList(transformDef)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`col12` STRING}, primaryKeys=col1, partitionKeys=col12, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 10], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 20], op=INSERT, meta=({op_ts=2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, 10], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 20], after=[2, x, 20], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @EnumSource
    void testOpTypeMetadataColumn(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.TRANSFORM_TABLE);
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
                        "*,concat(col1,'0') as col12,__data_event_type__ as rk,op_ts as opts",
                        "col1 <> '3'",
                        "col1",
                        "col12",
                        "key1=value1",
                        "",
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
                        new ArrayList<>(Collections.singletonList(transformDef)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`col12` STRING,`rk` STRING NOT NULL,`opts` BIGINT NOT NULL}, primaryKeys=col1, partitionKeys=col12, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 10, +I, 1], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 20, +I, 2], op=INSERT, meta=({op_ts=2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, 10, -D, 4], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 20, -U, 5], after=[2, x, 20, +U, 5], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @EnumSource
    void testTransformTwice(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.TRANSFORM_TABLE);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup transform
        TransformDef transformDef1 =
                new TransformDef(
                        "default_namespace.default_schema.table1",
                        "*,concat(col1,'1') as col12",
                        "col1 = '1' OR col1 = '999'",
                        "col1",
                        "col12",
                        "key1=value1",
                        "",
                        null);
        TransformDef transformDef2 =
                new TransformDef(
                        "default_namespace.default_schema.table1",
                        "*,concat(col1,'2') as col12",
                        "col1 = '2'",
                        null,
                        null,
                        null,
                        "",
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
                        new ArrayList<>(Arrays.asList(transformDef1, transformDef2)),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`col12` STRING}, primaryKeys=col1, partitionKeys=col12, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 11], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 22], op=INSERT, meta=({op_ts=2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, 11], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 22], after=[2, x, 22], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @EnumSource
    void testOneToOneRouting(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup route
        TableId routedTable1 = TableId.tableId("default_namespace", "default_schema", "routed1");
        TableId routedTable2 = TableId.tableId("default_namespace", "default_schema", "routed2");
        List<RouteDef> routeDef =
                Arrays.asList(
                        new RouteDef(TABLE_1.toString(), routedTable1.toString(), null, null),
                        new RouteDef(TABLE_2.toString(), routedTable2.toString(), null, null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        routeDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> routed1Results = ValuesDatabase.getResults(routedTable1);
        assertThat(routed1Results)
                .contains(
                        "default_namespace.default_schema.routed1:col1=2;newCol3=x",
                        "default_namespace.default_schema.routed1:col1=3;newCol3=");
        List<String> routed2Results = ValuesDatabase.getResults(routedTable2);
        assertThat(routed2Results)
                .contains(
                        "default_namespace.default_schema.routed2:col1=1;col2=1",
                        "default_namespace.default_schema.routed2:col1=2;col2=2",
                        "default_namespace.default_schema.routed2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.routed1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.routed2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.routed1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed2, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed2, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed2, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.routed1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.routed1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testIdenticalOneToOneRouting(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup route
        TableId routedTable1 = TABLE_1;
        TableId routedTable2 = TABLE_2;
        List<RouteDef> routeDef =
                Arrays.asList(
                        new RouteDef(TABLE_1.toString(), routedTable1.toString(), null, null),
                        new RouteDef(TABLE_2.toString(), routedTable2.toString(), null, null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        routeDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> routed1Results = ValuesDatabase.getResults(routedTable1);
        assertThat(routed1Results)
                .contains(
                        "default_namespace.default_schema.table1:col1=2;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;newCol3=");
        List<String> routed2Results = ValuesDatabase.getResults(routedTable2);
        assertThat(routed2Results)
                .contains(
                        "default_namespace.default_schema.table2:col1=1;col2=1",
                        "default_namespace.default_schema.table2:col1=2;col2=2",
                        "default_namespace.default_schema.table2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testMergingWithRoute(ValuesDataSink.SinkApi sinkApi) throws Exception {
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

        // Create test dataset:
        // Create table 1 [id, name, age]
        // Table 1: +I[1, Alice, 18]
        // Table 1: +I[2, Bob, 20]
        // Table 1: -U[2, Bob, 20] +U[2, Bob, 30]
        // Create table 2 [id, name, age, description]
        // Table 2: +I[3, Charlie, 15, student]
        // Table 2: +I[4, Donald, 25, student]
        // Table 2: -D[4, Donald, 25, student]
        // Rename column for table 1: name -> last_name
        // Add column for table 2: gender
        // Table 1: +I[5, Eliza, 24]
        // Table 2: +I[6, Frank, 30, student, male]
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
                                    BinaryStringData.fromString("Charlie"),
                                    (byte) 15,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(new RenameColumnEvent(myTable1, ImmutableMap.of("name", "last_name")));
        events.add(
                new AddColumnEvent(
                        myTable2,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("gender", DataTypes.STRING())))));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {5, BinaryStringData.fromString("Eliza"), 24})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.VARCHAR(255),
                                            DataTypes.TINYINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        })
                                .generate(
                                        new Object[] {
                                            6L,
                                            BinaryStringData.fromString("Frank"),
                                            (byte) 30,
                                            BinaryStringData.fromString("student"),
                                            BinaryStringData.fromString("male")
                                        })));

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup route
        TableId mergedTable = TableId.tableId("default_namespace", "default_schema", "merged");
        List<RouteDef> routeDef =
                Collections.singletonList(
                        new RouteDef(
                                "default_namespace.default_schema.mytable[0-9]",
                                mergedTable.toString(),
                                null,
                                null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        routeDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
        Schema mergedTableSchema = ValuesDatabase.getTableSchema(mergedTable);
        assertThat(mergedTableSchema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("description", DataTypes.STRING())
                                .physicalColumn("last_name", DataTypes.STRING())
                                .physicalColumn("gender", DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.merged, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`description` STRING, position=AFTER, existedColumnName=age}]}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.merged, typeMapping={id=BIGINT}, oldTypeMapping={id=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[3, Charlie, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[4, Donald, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[4, Donald, 25, student], after=[], op=DELETE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`last_name` STRING, position=AFTER, existedColumnName=description}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=AFTER, existedColumnName=last_name}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[5, null, 24, null, Eliza, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[6, Frank, 30, student, null, male], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testTransformMergingWithRoute(ValuesDataSink.SinkApi sinkApi) throws Exception {
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

        // Create test dataset:
        // Create table 1 [id, name, age]
        // Table 1: +I[1, Alice, 18]
        // Table 1: +I[2, Bob, 20]
        // Table 1: -U[2, Bob, 20] +U[2, Bob, 30]
        // Create table 2 [id, name, age, description]
        // Table 2: +I[3, Charlie, 15, student]
        // Table 2: +I[4, Donald, 25, student]
        // Table 2: -D[4, Donald, 25, student]
        // Rename column for table 1: name -> last_name
        // Add column for table 2: gender
        // Table 1: +I[5, Eliza, 24]
        // Table 2: +I[6, Frank, 30, student, male]
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
                                    BinaryStringData.fromString("Charlie"),
                                    (byte) 15,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        //        events.add(new RenameColumnEvent(myTable1, ImmutableMap.of("name", "last_name")));
        events.add(
                new AddColumnEvent(
                        myTable2,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("gender", DataTypes.STRING())))));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {5, BinaryStringData.fromString("Eliza"), 24})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.VARCHAR(255),
                                            DataTypes.TINYINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        })
                                .generate(
                                        new Object[] {
                                            6L,
                                            BinaryStringData.fromString("Frank"),
                                            (byte) 30,
                                            BinaryStringData.fromString("student"),
                                            BinaryStringData.fromString("male")
                                        })));

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup transform
        List<TransformDef> transformDef =
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.mytable[0-9]",
                                "*,'last_name' as last_name",
                                null,
                                null,
                                null,
                                null,
                                "",
                                null));

        // Setup route
        TableId mergedTable = TableId.tableId("default_namespace", "default_schema", "merged");
        List<RouteDef> routeDef =
                Collections.singletonList(
                        new RouteDef(
                                "default_namespace.default_schema.mytable[0-9]",
                                mergedTable.toString(),
                                null,
                                null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        routeDef,
                        transformDef,
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
        Schema mergedTableSchema = ValuesDatabase.getTableSchema(mergedTable);
        assertThat(mergedTableSchema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("last_name", DataTypes.STRING())
                                .physicalColumn("description", DataTypes.STRING())
                                .physicalColumn("gender", DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.merged, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`last_name` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[1, Alice, 18, last_name], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[2, Bob, 20, last_name], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[2, Bob, 20, last_name], after=[2, Bob, 30, last_name], op=UPDATE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`description` STRING, position=AFTER, existedColumnName=last_name}]}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.merged, typeMapping={id=BIGINT NOT NULL}, oldTypeMapping={id=INT NOT NULL}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[3, Charlie, 15, last_name, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[4, Donald, 25, last_name, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[4, Donald, 25, last_name, student], after=[], op=DELETE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=AFTER, existedColumnName=description}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[5, Eliza, 24, last_name, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[6, Frank, 30, last_name, student, male], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testTransformMergingWithRouteChangeOrder(ValuesDataSink.SinkApi sinkApi) throws Exception {
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

        // Create test dataset:
        // Create table 1 [id, name, age]
        // Create table 2 [id, name, age, description]
        // Table 1: +I[1, Alice, 18]
        // Table 1: +I[2, Bob, 20]
        // Table 1: -U[2, Bob, 20] +U[2, Bob, 30]
        // Table 2: +I[3, Charlie, 15, student]
        // Table 2: +I[4, Donald, 25, student]
        // Table 2: -D[4, Donald, 25, student]
        // Rename column for table 1: name -> last_name
        // Add column for table 2: gender
        // Table 1: +I[5, Eliza, 24]
        // Table 2: +I[6, Frank, 30, student, male]
        List<Event> events = new ArrayList<>();
        BinaryRecordDataGenerator table1dataGenerator =
                new BinaryRecordDataGenerator(
                        table1Schema.getColumnDataTypes().toArray(new DataType[0]));
        BinaryRecordDataGenerator table2dataGenerator =
                new BinaryRecordDataGenerator(
                        table2Schema.getColumnDataTypes().toArray(new DataType[0]));
        events.add(new CreateTableEvent(myTable1, table1Schema));
        events.add(new CreateTableEvent(myTable2, table2Schema));
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
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    3L,
                                    BinaryStringData.fromString("Charlie"),
                                    (byte) 15,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        events.add(
                DataChangeEvent.deleteEvent(
                        myTable2,
                        table2dataGenerator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("Donald"),
                                    (byte) 25,
                                    BinaryStringData.fromString("student")
                                })));
        //        events.add(new RenameColumnEvent(myTable1, ImmutableMap.of("name", "last_name")));
        events.add(
                new AddColumnEvent(
                        myTable2,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("gender", DataTypes.STRING())))));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable1,
                        table1dataGenerator.generate(
                                new Object[] {5, BinaryStringData.fromString("Eliza"), 24})));
        events.add(
                DataChangeEvent.insertEvent(
                        myTable2,
                        new BinaryRecordDataGenerator(
                                        new DataType[] {
                                            DataTypes.BIGINT(),
                                            DataTypes.VARCHAR(255),
                                            DataTypes.TINYINT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        })
                                .generate(
                                        new Object[] {
                                            6L,
                                            BinaryStringData.fromString("Frank"),
                                            (byte) 30,
                                            BinaryStringData.fromString("student"),
                                            BinaryStringData.fromString("male")
                                        })));

        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup transform
        List<TransformDef> transformDef =
                Collections.singletonList(
                        new TransformDef(
                                "default_namespace.default_schema.mytable[0-9]",
                                "*,'last_name' as last_name",
                                null,
                                null,
                                null,
                                null,
                                "",
                                null));

        // Setup route
        TableId mergedTable = TableId.tableId("default_namespace", "default_schema", "merged");
        List<RouteDef> routeDef =
                Collections.singletonList(
                        new RouteDef(
                                "default_namespace.default_schema.mytable[0-9]",
                                mergedTable.toString(),
                                null,
                                null));

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        routeDef,
                        transformDef,
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
        Schema mergedTableSchema = ValuesDatabase.getTableSchema(mergedTable);
        assertThat(mergedTableSchema)
                .isEqualTo(
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT().notNull())
                                .physicalColumn("name", DataTypes.STRING())
                                .physicalColumn("age", DataTypes.INT())
                                .physicalColumn("last_name", DataTypes.STRING())
                                .physicalColumn("description", DataTypes.STRING())
                                .physicalColumn("gender", DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.merged, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`last_name` STRING}, primaryKeys=id, options=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`description` STRING, position=AFTER, existedColumnName=last_name}]}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.merged, typeMapping={id=BIGINT NOT NULL}, oldTypeMapping={id=INT NOT NULL}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[1, Alice, 18, last_name, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[2, Bob, 20, last_name, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[2, Bob, 20, last_name, null], after=[2, Bob, 30, last_name, null], op=UPDATE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[3, Charlie, 15, last_name, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[4, Donald, 25, last_name, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[4, Donald, 25, last_name, student], after=[], op=DELETE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=AFTER, existedColumnName=description}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[5, Eliza, 24, last_name, null, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[6, Frank, 30, last_name, student, male], op=INSERT, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testRouteWithReplaceSymbol(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
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
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.singletonList(
                                new RouteDef(
                                        "default_namespace.default_schema.table[0-9]",
                                        "replaced_namespace.replaced_schema.__$__",
                                        "__$__",
                                        null)),
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
                        "CreateTableEvent{tableId=replaced_namespace.replaced_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "CreateTableEvent{tableId=replaced_namespace.replaced_schema.table2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=replaced_namespace.replaced_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table2, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table2, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table2, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "RenameColumnEvent{tableId=replaced_namespace.replaced_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=replaced_namespace.replaced_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testMergingTemporalTypesWithPromotedPrecisions(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        List<Event> events = generateTemporalColumnEvents("default_table_");
        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        pipelineConfig.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "America/New_York");
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Arrays.asList(
                                new RouteDef(
                                        "default_namespace.default_schema.default_table_ts_\\.*",
                                        "default_namespace.default_schema.default_table_timestamp_merged",
                                        null,
                                        "Merge timestamp columns with different precision"),
                                new RouteDef(
                                        "default_namespace.default_schema.default_table_tz_\\.*",
                                        "default_namespace.default_schema.default_table_zoned_timestamp_merged",
                                        null,
                                        "Merge timestamp_tz columns with different precision"),
                                new RouteDef(
                                        "default_namespace.default_schema.default_table_ltz_\\.*",
                                        "default_namespace.default_schema.default_table_local_zoned_timestamp_merged",
                                        null,
                                        "Merge timestamp_ltz columns with different precision"),
                                new RouteDef(
                                        "default_namespace.default_schema.default_table_\\.*",
                                        "default_namespace.default_schema.default_everything_merged",
                                        null,
                                        "Merge all timestamp family columns with different precision")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        String[] expected =
                Stream.of(
                                // Merging timestamp with different precision
                                "CreateTableEvent{tableId={}_table_timestamp_merged, schema=columns={`id` INT,`name` STRING,`age` INT,`birthday` TIMESTAMP(0)}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId={}_table_timestamp_merged, before=[], after=[1, Alice, 17, 2020-01-01T14:28:57], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}_table_timestamp_merged, typeMapping={birthday=TIMESTAMP(9)}, oldTypeMapping={birthday=TIMESTAMP(0)}}",
                                "DataChangeEvent{tableId={}_table_timestamp_merged, before=[], after=[2, Alice, 17, 2020-01-01T14:28:57.123456789], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_table_timestamp_merged, before=[], after=[101, Zen, 19, 2020-01-01T14:28:57], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_table_timestamp_merged, before=[], after=[102, Zen, 19, 2020-01-01T14:28:57.123456789], op=INSERT, meta=()}",

                                // Merging zoned timestamp with different precision
                                "CreateTableEvent{tableId={}_table_zoned_timestamp_merged, schema=columns={`id` INT,`name` STRING,`age` INT,`birthday` TIMESTAMP(0) WITH TIME ZONE}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId={}_table_zoned_timestamp_merged, before=[], after=[3, Alice, 17, 2020-01-01T14:28:57Z], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}_table_zoned_timestamp_merged, typeMapping={birthday=TIMESTAMP(9) WITH TIME ZONE}, oldTypeMapping={birthday=TIMESTAMP(0) WITH TIME ZONE}}",
                                "DataChangeEvent{tableId={}_table_zoned_timestamp_merged, before=[], after=[4, Alice, 17, 2020-01-01T14:28:57.123456789Z], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_table_zoned_timestamp_merged, before=[], after=[103, Zen, 19, 2020-01-01T14:28:57Z], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_table_zoned_timestamp_merged, before=[], after=[104, Zen, 19, 2020-01-01T14:28:57.123456789Z], op=INSERT, meta=()}",

                                // Merging local-zoned timestamp with different precision
                                "CreateTableEvent{tableId={}_table_local_zoned_timestamp_merged, schema=columns={`id` INT,`name` STRING,`age` INT,`birthday` TIMESTAMP_LTZ(0)}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId={}_table_local_zoned_timestamp_merged, before=[], after=[5, Alice, 17, 2020-01-01T14:28:57], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}_table_local_zoned_timestamp_merged, typeMapping={birthday=TIMESTAMP_LTZ(9)}, oldTypeMapping={birthday=TIMESTAMP_LTZ(0)}}",
                                "DataChangeEvent{tableId={}_table_local_zoned_timestamp_merged, before=[], after=[6, Alice, 17, 2020-01-01T14:28:57.123456789], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_table_local_zoned_timestamp_merged, before=[], after=[105, Zen, 19, 2020-01-01T14:28:57], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_table_local_zoned_timestamp_merged, before=[], after=[106, Zen, 19, 2020-01-01T14:28:57.123456789], op=INSERT, meta=()}",

                                // Merging all
                                "CreateTableEvent{tableId={}_everything_merged, schema=columns={`id` INT,`name` STRING,`age` INT,`birthday` TIMESTAMP(0)}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[1, Alice, 17, 2020-01-01T14:28:57], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}_everything_merged, typeMapping={birthday=TIMESTAMP(9)}, oldTypeMapping={birthday=TIMESTAMP(0)}}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[2, Alice, 17, 2020-01-01T14:28:57.123456789], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}_everything_merged, typeMapping={birthday=TIMESTAMP(9) WITH TIME ZONE}, oldTypeMapping={birthday=TIMESTAMP(9)}}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[3, Alice, 17, 2020-01-01T14:28:57Z], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[4, Alice, 17, 2020-01-01T14:28:57.123456789Z], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[5, Alice, 17, 2020-01-01T04:28:57-05:00], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[6, Alice, 17, 2020-01-01T04:28:57.123456789-05:00], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[101, Zen, 19, 2020-01-01T09:28:57-05:00], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[102, Zen, 19, 2020-01-01T09:28:57.123456789-05:00], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[103, Zen, 19, 2020-01-01T14:28:57Z], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[104, Zen, 19, 2020-01-01T14:28:57.123456789Z], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[105, Zen, 19, 2020-01-01T04:28:57-05:00], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}_everything_merged, before=[], after=[106, Zen, 19, 2020-01-01T04:28:57.123456789-05:00], op=INSERT, meta=()}")
                        .map(s -> s.replace("{}", "default_namespace.default_schema.default"))
                        .toArray(String[]::new);

        assertThat(outputEvents).containsExactlyInAnyOrder(expected);
    }

    @ParameterizedTest
    @EnumSource
    void testMergingDecimalWithVariousPrecisions(ValuesDataSink.SinkApi sinkApi) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        List<Event> events = generateDecimalColumnEvents("default_table_");
        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.SINK_API, sinkApi);
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
                        Collections.singletonList(
                                new RouteDef(
                                        "default_namespace.default_schema.default_table_\\.*",
                                        "default_namespace.default_schema.default_everything_merged",
                                        null,
                                        "Merge all decimal columns with different precision")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");

        String[] expected =
                Stream.of(
                                "CreateTableEvent{tableId={}, schema=columns={`id` INT,`name` STRING,`age` INT,`fav_num` TINYINT}, primaryKeys=id, options=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[1, Alice, 17, 1], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}, typeMapping={fav_num=SMALLINT}, oldTypeMapping={fav_num=TINYINT}}",
                                "DataChangeEvent{tableId={}, before=[], after=[2, Alice, 17, 22], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}, typeMapping={fav_num=INT}, oldTypeMapping={fav_num=SMALLINT}}",
                                "DataChangeEvent{tableId={}, before=[], after=[3, Alice, 17, 3333], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}, typeMapping={fav_num=BIGINT}, oldTypeMapping={fav_num=INT}}",
                                "DataChangeEvent{tableId={}, before=[], after=[4, Alice, 17, 44444444], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}, typeMapping={fav_num=DECIMAL(19, 0)}, oldTypeMapping={fav_num=BIGINT}}",
                                "DataChangeEvent{tableId={}, before=[], after=[5, Alice, 17, 555555555555555], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}, typeMapping={fav_num=DECIMAL(24, 5)}, oldTypeMapping={fav_num=DECIMAL(19, 0)}}",
                                "DataChangeEvent{tableId={}, before=[], after=[6, Alice, 17, 66666.66666], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[7, Alice, 17, 77777777.17000], op=INSERT, meta=()}",
                                "AlterColumnTypeEvent{tableId={}, typeMapping={fav_num=DECIMAL(38, 19)}, oldTypeMapping={fav_num=DECIMAL(24, 5)}}",
                                "DataChangeEvent{tableId={}, before=[], after=[8, Alice, 17, 888888888.8888888888888888888], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[101, Zen, 19, 1.0000000000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[102, Zen, 19, 22.0000000000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[103, Zen, 19, 3333.0000000000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[104, Zen, 19, 44444444.0000000000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[105, Zen, 19, 555555555555555.0000000000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[106, Zen, 19, 66666.6666600000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[107, Zen, 19, 77777777.1700000000000000000], op=INSERT, meta=()}",
                                "DataChangeEvent{tableId={}, before=[], after=[108, Zen, 19, 888888888.8888888888888888888], op=INSERT, meta=()}")
                        .map(
                                s ->
                                        s.replace(
                                                "{}",
                                                "default_namespace.default_schema.default_everything_merged"))
                        .toArray(String[]::new);

        assertThat(outputEvents).containsExactlyInAnyOrder(expected);
    }

    private List<Event> generateTemporalColumnEvents(String tableNamePrefix) {
        List<Event> events = new ArrayList<>();

        // Initialize schemas
        List<String> names = Arrays.asList("ts_0", "ts_9", "tz_0", "tz_9", "ltz_0", "ltz_9");

        List<DataType> types =
                Arrays.asList(
                        DataTypes.TIMESTAMP(0),
                        DataTypes.TIMESTAMP(9),
                        DataTypes.TIMESTAMP_TZ(0),
                        DataTypes.TIMESTAMP_TZ(9),
                        DataTypes.TIMESTAMP_LTZ(0),
                        DataTypes.TIMESTAMP_LTZ(9));

        Instant lowPrecisionTimestamp = Instant.parse("2020-01-01T14:28:57Z");
        Instant highPrecisionTimestamp = Instant.parse("2020-01-01T14:28:57.123456789Z");

        List<Object> values =
                Arrays.asList(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(lowPrecisionTimestamp, ZoneId.of("UTC"))),
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.ofInstant(highPrecisionTimestamp, ZoneId.of("UTC"))),
                        ZonedTimestampData.fromZonedDateTime(
                                ZonedDateTime.ofInstant(lowPrecisionTimestamp, ZoneId.of("UTC"))),
                        ZonedTimestampData.fromZonedDateTime(
                                ZonedDateTime.ofInstant(highPrecisionTimestamp, ZoneId.of("UTC"))),
                        LocalZonedTimestampData.fromInstant(lowPrecisionTimestamp),
                        LocalZonedTimestampData.fromInstant(highPrecisionTimestamp));

        List<Schema> schemas =
                types.stream()
                        .map(
                                temporalColumnType ->
                                        Schema.newBuilder()
                                                .physicalColumn("id", DataTypes.INT())
                                                .physicalColumn("name", DataTypes.STRING())
                                                .physicalColumn("age", DataTypes.INT())
                                                .physicalColumn("birthday", temporalColumnType)
                                                .primaryKey("id")
                                                .build())
                        .collect(Collectors.toList());

        for (int i = 0; i < names.size(); i++) {
            TableId generatedTableId =
                    TableId.tableId(
                            "default_namespace", "default_schema", tableNamePrefix + names.get(i));
            Schema generatedSchema = schemas.get(i);
            events.add(new CreateTableEvent(generatedTableId, generatedSchema));
            events.add(
                    DataChangeEvent.insertEvent(
                            generatedTableId,
                            generate(generatedSchema, 1 + i, "Alice", 17, values.get(i))));
        }

        for (int i = 0; i < names.size(); i++) {
            TableId generatedTableId =
                    TableId.tableId(
                            "default_namespace", "default_schema", tableNamePrefix + names.get(i));
            Schema generatedSchema = schemas.get(i);
            events.add(
                    DataChangeEvent.insertEvent(
                            generatedTableId,
                            generate(generatedSchema, 101 + i, "Zen", 19, values.get(i))));
        }

        return events;
    }

    private List<Event> generateDecimalColumnEvents(String tableNamePrefix) {
        List<Event> events = new ArrayList<>();

        // Initialize schemas
        List<String> names =
                Arrays.asList(
                        "tiny",
                        "small",
                        "vanilla",
                        "big",
                        "dec_15_0",
                        "decimal_10_10",
                        "decimal_16_2",
                        "decimal_29_19");

        List<DataType> types =
                Arrays.asList(
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.DECIMAL(15, 0),
                        DataTypes.DECIMAL(10, 5),
                        DataTypes.DECIMAL(16, 2),
                        DataTypes.DECIMAL(29, 19));

        List<Object> values =
                Arrays.asList(
                        (byte) 1,
                        (short) 22,
                        3333,
                        (long) 44444444,
                        DecimalData.fromBigDecimal(new BigDecimal("555555555555555"), 15, 0),
                        DecimalData.fromBigDecimal(new BigDecimal("66666.66666"), 10, 5),
                        DecimalData.fromBigDecimal(new BigDecimal("77777777.17"), 16, 2),
                        DecimalData.fromBigDecimal(
                                new BigDecimal("888888888.8888888888888888888"), 29, 19));

        List<Schema> schemas =
                types.stream()
                        .map(
                                temporalColumnType ->
                                        Schema.newBuilder()
                                                .physicalColumn("id", DataTypes.INT())
                                                .physicalColumn("name", DataTypes.STRING())
                                                .physicalColumn("age", DataTypes.INT())
                                                .physicalColumn("fav_num", temporalColumnType)
                                                .primaryKey("id")
                                                .build())
                        .collect(Collectors.toList());

        for (int i = 0; i < names.size(); i++) {
            TableId generatedTableId =
                    TableId.tableId(
                            "default_namespace", "default_schema", tableNamePrefix + names.get(i));
            Schema generatedSchema = schemas.get(i);
            events.add(new CreateTableEvent(generatedTableId, generatedSchema));
            events.add(
                    DataChangeEvent.insertEvent(
                            generatedTableId,
                            generate(generatedSchema, 1 + i, "Alice", 17, values.get(i))));
        }

        for (int i = 0; i < names.size(); i++) {
            TableId generatedTableId =
                    TableId.tableId(
                            "default_namespace", "default_schema", tableNamePrefix + names.get(i));
            Schema generatedSchema = schemas.get(i);
            events.add(
                    DataChangeEvent.insertEvent(
                            generatedTableId,
                            generate(generatedSchema, 101 + i, "Zen", 19, values.get(i))));
        }

        return events;
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
}
