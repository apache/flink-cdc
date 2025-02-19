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
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Integration test for UDFs. */
class FlinkPipelineUdfITCase {
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

    // ----------------------
    // CDC pipeline UDF tests
    // ----------------------
    @ParameterizedTest
    @MethodSource("testParams")
    void testTransformWithUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*,format('from %s to %s is %s', col1, 'z', 'lie') AS fmt",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "format",
                        String.format(
                                "org.apache.flink.cdc.udf.examples.%s.FormatFunctionClass",
                                language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`fmt` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, from 1 to z is lie], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, from 2 to z is lie], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, from 3 to z is lie], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, from 1 to z is lie], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , from 2 to z is lie], after=[2, x, from 2 to z is lie], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testFilterWithUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*,addone(col1) as collen",
                        "addone(col1) <> '2'",
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "addone",
                        String.format(
                                "org.apache.flink.cdc.udf.examples.%s.AddOneFunctionClass",
                                language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`collen` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 3], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, 4], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 3], after=[2, x, 3], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testOverloadedUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, typeof(true) as tob, typeof(1) as toi, typeof(3.14) as tof, typeof('str') as tos",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "typeof",
                        String.format(
                                "org.apache.flink.cdc.udf.examples.%s.TypeOfFunctionClass",
                                language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`tob` STRING,`toi` STRING,`tof` STRING,`tos` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, Boolean: true, Integer: 1, Double: 3.14, String: str], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, Boolean: true, Integer: 1, Double: 3.14, String: str], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, Boolean: true, Integer: 1, Double: 3.14, String: str], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, Boolean: true, Integer: 1, Double: 3.14, String: str], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , Boolean: true, Integer: 1, Double: 3.14, String: str], after=[2, x, Boolean: true, Integer: 1, Double: 3.14, String: str], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUdfLifecycle(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, lifecycle() as stt",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "lifecycle",
                        String.format(
                                "org.apache.flink.cdc.udf.examples.%s.LifecycleFunctionClass",
                                language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .contains("[ LifecycleFunction ] opened.")
                .contains(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`stt` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, #0], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, #1], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, #2], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, #3], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , #4], after=[2, x, #5], op=UPDATE, meta=({op_ts=5})}")
                .contains("[ LifecycleFunction ] closed. Called 6 times.");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testTypeHintedUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, answer() as ans",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "answer",
                        String.format(
                                "org.apache.flink.cdc.udf.examples.%s.TypeHintFunctionClass",
                                language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`ans` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, Forty-two], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, Forty-two], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, Forty-two], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, Forty-two], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , Forty-two], after=[2, x, Forty-two], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testComplicatedUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, addone(addone(col1)) as inccol, typeof(42) as typ, format('%s-%d', col1, 42) as fmt, lifecycle() as stt",
                        null,
                        "col1",
                        null,
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
                        Collections.singletonList(transformDef),
                        Arrays.asList(
                                new UdfDef(
                                        "lifecycle",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.LifecycleFunctionClass",
                                                language)),
                                new UdfDef(
                                        "addone",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.AddOneFunctionClass",
                                                language)),
                                new UdfDef(
                                        "typeof",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.TypeOfFunctionClass",
                                                language)),
                                new UdfDef(
                                        "format",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.FormatFunctionClass",
                                                language))),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .contains("[ LifecycleFunction ] opened.")
                .contains(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`inccol` STRING,`typ` STRING,`fmt` STRING,`stt` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 3, Integer: 42, 1-42, #0], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 4, Integer: 42, 2-42, #1], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, 5, Integer: 42, 3-42, #2], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, 3, Integer: 42, 1-42, #3], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 4, Integer: 42, 2-42, #4], after=[2, x, 4, Integer: 42, 2-42, #5], op=UPDATE, meta=({op_ts=5})}")
                .contains("[ LifecycleFunction ] closed. Called 6 times.");
    }

    // --------------------------
    // Flink-compatible UDF tests
    // --------------------------
    @ParameterizedTest
    @MethodSource("testParams")
    void testTransformWithFlinkUdf(ValuesDataSink.SinkApi sinkApi, String language)
            throws Exception {
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
                        "*,format('from %s to %s is %s', col1, 'z', 'lie') AS fmt",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "format",
                        String.format(
                                "org.apache.flink.udf.examples.%s.FormatFunctionClass", language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`fmt` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, from 1 to z is lie], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, from 2 to z is lie], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, from 3 to z is lie], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, from 1 to z is lie], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , from 2 to z is lie], after=[2, x, from 2 to z is lie], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testFilterWithFlinkUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*,addone(col1) as collen",
                        "addone(col1) <> '2'",
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "addone",
                        String.format(
                                "org.apache.flink.udf.examples.%s.AddOneFunctionClass", language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`collen` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 3], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, 4], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 3], after=[2, x, 3], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testOverloadedFlinkUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, typeof(true) as tob, typeof(1) as toi, typeof(3.14) as tof, typeof('str') as tos",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        UdfDef udfDef =
                new UdfDef(
                        "typeof",
                        String.format(
                                "org.apache.flink.udf.examples.%s.TypeOfFunctionClass", language));

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
                        Collections.singletonList(udfDef),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`tob` STRING,`toi` STRING,`tof` STRING,`tos` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, Boolean: true, Integer: 1, Double: 3.14, String: str], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, Boolean: true, Integer: 1, Double: 3.14, String: str], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, Boolean: true, Integer: 1, Double: 3.14, String: str], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, Boolean: true, Integer: 1, Double: 3.14, String: str], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , Boolean: true, Integer: 1, Double: 3.14, String: str], after=[2, x, Boolean: true, Integer: 1, Double: 3.14, String: str], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testComplicatedFlinkUdf(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, addone(addone(col1)) as inccol, typeof(42) as typ, format('%s-%d', col1, 42) as fmt",
                        null,
                        "col1",
                        null,
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
                        Collections.singletonList(transformDef),
                        Arrays.asList(
                                new UdfDef(
                                        "addone",
                                        String.format(
                                                "org.apache.flink.udf.examples.%s.AddOneFunctionClass",
                                                language)),
                                new UdfDef(
                                        "typeof",
                                        String.format(
                                                "org.apache.flink.udf.examples.%s.TypeOfFunctionClass",
                                                language)),
                                new UdfDef(
                                        "format",
                                        String.format(
                                                "org.apache.flink.udf.examples.%s.FormatFunctionClass",
                                                language))),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`inccol` STRING,`typ` STRING,`fmt` STRING}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, 3, Integer: 42, 1-42], op=INSERT, meta=({op_ts=1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, 4, Integer: 42, 2-42], op=INSERT, meta=({op_ts=2})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, 5, Integer: 42, 3-42], op=INSERT, meta=({op_ts=3})}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, 3, Integer: 42, 1-42], after=[], op=DELETE, meta=({op_ts=4})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , 4, Integer: 42, 2-42], after=[2, x, 4, Integer: 42, 2-42], op=UPDATE, meta=({op_ts=5})}");
    }

    @ParameterizedTest
    @MethodSource("testParams")
    @Disabled("For manual test as there is a limit for quota.")
    void testTransformWithModel(ValuesDataSink.SinkApi sinkApi, String language) throws Exception {
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
                        "*, CHAT(col1) AS emb",
                        null,
                        "col1",
                        null,
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
                        Collections.singletonList(transformDef),
                        new ArrayList<>(),
                        Arrays.asList(
                                new ModelDef(
                                        "CHAT",
                                        "OpenAIChatModel",
                                        new LinkedHashMap<>(
                                                ImmutableMap.<String, String>builder()
                                                        .put("openai.model", "gpt-4o-mini")
                                                        .put(
                                                                "openai.host",
                                                                "http://langchain4j.dev/demo/openai/v1")
                                                        .put("openai.apikey", "demo")
                                                        .build()))),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .contains(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`emb` STRING}, primaryKeys=col1, options=({key1=value1})}")
                // The result of transform by model is not fixed.
                .hasSize(9);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testComplicatedUdfReturnTypes(ValuesDataSink.SinkApi sinkApi, String language)
            throws Exception {
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

        // Setup transform
        TransformDef transformDef =
                new TransformDef(
                        "default_namespace.default_schema.table1",
                        "*, get_char() AS char_col, get_varchar() AS varchar_col, get_binary() AS binary_col, get_varbinary() AS varbinary_col, get_ts() AS ts_col, get_ts_ltz() AS ts_ltz_col, get_decimal() AS decimal_col, get_non_null() AS non_null_col",
                        null,
                        "col1",
                        null,
                        "key1=value1",
                        "",
                        null);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "America/Los_Angeles");
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(transformDef),
                        Arrays.asList(
                                new UdfDef(
                                        "get_char",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.CharTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_varchar",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.VarCharTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_binary",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.BinaryTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_varbinary",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.VarBinaryTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_ts",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.TimestampTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_ts_ltz",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.LocalZonedTimestampTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_decimal",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.DecimalTypeReturningClass",
                                                language)),
                                new UdfDef(
                                        "get_non_null",
                                        String.format(
                                                "org.apache.flink.cdc.udf.examples.%s.precision.DecimalTypeNonNullReturningClass",
                                                language))),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        // Check the order and content of all received events
        String[] outputEvents = outCaptor.toString().trim().split("\n");
        assertThat(outputEvents)
                .contains(
                        "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`col2` STRING,`char_col` STRING,`varchar_col` STRING,`binary_col` BINARY(17),`varbinary_col` VARBINARY(17),`ts_col` TIMESTAMP(2),`ts_ltz_col` TIMESTAMP_LTZ(2),`decimal_col` DECIMAL(10, 3),`non_null_col` DECIMAL(10, 3)}, primaryKeys=col1, options=({key1=value1})}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1, This is a string., This is a string., eHl6enk=, eHl6enk=, 1970-01-02T00:00, 1970-01-02T00:00, 12.315, 12.315], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2, This is a string., This is a string., eHl6enk=, eHl6enk=, 1970-01-02T00:00, 1970-01-02T00:00, 12.315, 12.315], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3, This is a string., This is a string., eHl6enk=, eHl6enk=, 1970-01-02T00:00, 1970-01-02T00:00, 12.315, 12.315], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=AFTER, existedColumnName=col2}]}",
                        "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                        "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, This is a string., This is a string., eHl6enk=, eHl6enk=, 1970-01-02T00:00, 1970-01-02T00:00, 12.315, 12.315], after=[], op=DELETE, meta=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, , This is a string., This is a string., eHl6enk=, eHl6enk=, 1970-01-02T00:00, 1970-01-02T00:00, 12.315, 12.315], after=[2, x, This is a string., This is a string., eHl6enk=, eHl6enk=, 1970-01-02T00:00, 1970-01-02T00:00, 12.315, 12.315], op=UPDATE, meta=()}");
    }

    private static Stream<Arguments> testParams() {
        return Stream.of(
                arguments(ValuesDataSink.SinkApi.SINK_FUNCTION, "java"),
                arguments(ValuesDataSink.SinkApi.SINK_V2, "java"),
                arguments(ValuesDataSink.SinkApi.SINK_FUNCTION, "scala"),
                arguments(ValuesDataSink.SinkApi.SINK_V2, "scala"));
    }
}
