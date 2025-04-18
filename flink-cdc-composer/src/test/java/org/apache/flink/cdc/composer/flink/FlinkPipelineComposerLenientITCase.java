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
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_1;
import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.TABLE_2;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link FlinkPipelineComposer}. */
class FlinkPipelineComposerLenientITCase {

    private static final int MAX_PARALLELISM = 4;

    private static final String LINE_SEPARATOR = System.lineSeparator();

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
                        "default_namespace.default_schema.table1:col1=2;col2=;col3=;newCol2=;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;col2=3;col3=;newCol2=;newCol3=");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, null, null, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, null, null, ], after=[2, null, null, null, x], op=UPDATE, meta=()}");
    }

    @ParameterizedTest
    @EnumSource
    void testSingleSplitSingleTableWithDefaultValue(ValuesDataSink.SinkApi sinkApi)
            throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_SINGLE_TABLE_WITH_DEFAULT_VALUE);
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
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check result in ValuesDatabase
        List<String> results = ValuesDatabase.getResults(TABLE_1);
        assertThat(results)
                .containsExactlyInAnyOrder(
                        "default_namespace.default_schema.table1:col1=2;col2=;col3=;newCol2=;newCol3=x;colWithDefault=;newColWithDefault=",
                        "default_namespace.default_schema.table1:col1=3;col2=3;col3=;newCol2=;newCol3=;colWithDefault=;newColWithDefault=");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, null, null, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, null, null, ], after=[2, null, null, null, x], op=UPDATE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`colWithDefault` STRING 'flink', position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newColWithDefault` STRING 'flink', position=LAST, existedColumnName=null}]}");
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
                        "default_namespace.default_schema.table1:col1=2;col2=;col3=;newCol2=;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;col2=3;col3=;newCol2=;newCol3=");
        List<String> table2Results = ValuesDatabase.getResults(TABLE_2);
        assertThat(table2Results)
                .contains(
                        "default_namespace.default_schema.table2:col1=1;col2=1",
                        "default_namespace.default_schema.table2:col1=2;col2=2",
                        "default_namespace.default_schema.table2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
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
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, null, null, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, null, null, 2], after=[2, null, null, null, x], op=UPDATE, meta=()}");
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
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`col12` STRING}, primaryKeys=col1, partitionKeys=col12, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 10], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 20], op=INSERT, meta=({op_ts=2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, 10, null, null, 1], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, 20, null, null, ], after=[2, null, 20, null, null, x], op=UPDATE, meta=({op_ts=5})}");
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
                        "*,concat(col1,'0') as col12,__data_event_type__ as rk",
                        "col1 <> '3'",
                        "col1",
                        "col12",
                        "key1=value1",
                        "",
                        null);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);

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
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`col12` STRING,`rk` STRING NOT NULL}, primaryKeys=col1, partitionKeys=col12, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 10, +I], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 20, +I], op=INSERT, meta=({op_ts=2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, 10, -D, null, null, 1], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, 20, -U, null, null, ], after=[2, null, 20, +U, null, null, x], op=UPDATE, meta=({op_ts=5})}");
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
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`col12` STRING}, primaryKeys=col1, partitionKeys=col12, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 11], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 22], op=INSERT, meta=({op_ts=2})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, 11, null, null, 1], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, 22, null, null, ], after=[2, null, 22, null, null, x], op=UPDATE, meta=({op_ts=5})}");
    }

    @Test
    void testOneToOneRouting() throws Exception {
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
                        "default_namespace.default_schema.routed1:col1=2;col2=;col3=;newCol2=;newCol3=x",
                        "default_namespace.default_schema.routed1:col1=3;col2=3;col3=;newCol2=;newCol3=");
        List<String> routed2Results = ValuesDatabase.getResults(routedTable2);
        assertThat(routed2Results)
                .contains(
                        "default_namespace.default_schema.routed2:col1=1;col2=1",
                        "default_namespace.default_schema.routed2:col1=2;col2=2",
                        "default_namespace.default_schema.routed2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
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
                        "AddColumnEvent{tableId=default_namespace.default_schema.routed1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[1, null, null, null, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.routed1, before=[2, null, null, null, 2], after=[2, null, null, null, x], op=UPDATE, meta=()}");
    }

    @Test
    void testIdenticalOneToOneRouting() throws Exception {
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
                        "default_namespace.default_schema.table1:col1=2;col2=;col3=;newCol2=;newCol3=x",
                        "default_namespace.default_schema.table1:col1=3;col2=3;col3=;newCol2=;newCol3=");
        List<String> routed2Results = ValuesDatabase.getResults(routedTable2);
        assertThat(routed2Results)
                .contains(
                        "default_namespace.default_schema.table2:col1=1;col2=1",
                        "default_namespace.default_schema.table2:col1=2;col2=2",
                        "default_namespace.default_schema.table2:col1=3;col2=3");

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
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
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, null, null, null, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, null, null, null, 2], after=[2, null, null, null, x], op=UPDATE, meta=()}");
    }

    @Test
    void testMergingWithRoute() throws Exception {
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
        // Create table 2 [id, name, age]
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
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.merged, schema=columns={`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[1, Alice, 18], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[2, Bob, 20], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[2, Bob, 20], after=[2, Bob, 30], op=UPDATE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`description` STRING, position=LAST, existedColumnName=null}]}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.merged, typeMapping={id=BIGINT}, oldTypeMapping={id=INT}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[3, Charlie, 15, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[4, Donald, 25, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[4, Donald, 25, student], after=[], op=DELETE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`last_name` STRING, position=LAST, existedColumnName=null}]}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[5, null, 24, null, Eliza, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[6, Frank, 30, student, null, male], op=INSERT, meta=()}");
    }

    @Test
    void testTransformMergingWithRoute() throws Exception {
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
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.merged, schema=columns={`id` INT NOT NULL,`name` STRING,`age` INT,`last_name` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[1, Alice, 18, last_name], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[2, Bob, 20, last_name], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[2, Bob, 20, last_name], after=[2, Bob, 30, last_name], op=UPDATE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`description` STRING, position=LAST, existedColumnName=null}]}",
                        "AlterColumnTypeEvent{tableId=default_namespace.default_schema.merged, typeMapping={id=BIGINT NOT NULL}, oldTypeMapping={id=INT NOT NULL}}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[3, Charlie, 15, last_name, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[], after=[4, Donald, 25, last_name, student], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.merged, before=[4, Donald, 25, last_name, student], after=[], op=DELETE, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.merged, addedColumns=[ColumnWithPosition{column=`gender` STRING, position=LAST, existedColumnName=null}]}",
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
        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
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
                        "AddColumnEvent{tableId=replaced_namespace.replaced_schema.table1, addedColumns=[ColumnWithPosition{column=`newCol2` STRING, position=LAST, existedColumnName=null}, ColumnWithPosition{column=`newCol3` STRING, position=LAST, existedColumnName=null}]}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[1, null, null, null, 1], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=replaced_namespace.replaced_schema.table1, before=[2, null, null, null, 2], after=[2, null, null, null, x], op=UPDATE, meta=()}");
    }
}
