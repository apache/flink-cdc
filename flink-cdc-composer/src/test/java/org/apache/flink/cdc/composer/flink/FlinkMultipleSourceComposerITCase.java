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
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.EventSetId;
import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link FlinkPipelineComposer}. */
class FlinkMultipleSourceComposerITCase {

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
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
    }

    @Test
    @EnumSource
    void testSingleSplitMultipleSources() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup value source
        Configuration sourceConfig1 = new Configuration();
        sourceConfig1.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                EventSetId.SINGLE_SPLIT_MULTI_SOURCE_TABLE_FIRST);
        Configuration sourceConfig2 = new Configuration();
        sourceConfig2.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                EventSetId.SINGLE_SPLIT_MULTI_SOURCE_TABLE_SECOND);
        SourceDef sourceDef1 =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source1", sourceConfig1);
        SourceDef sourceDef2 =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source2", sourceConfig2);

        // Setup value sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.LENIENT);
        List<SourceDef> sourceDefs = new ArrayList<>();
        sourceDefs.add(sourceDef1);
        sourceDefs.add(sourceDef2);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDefs,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();
        List<String> table1Results = ValuesDatabase.getAllResults();

        assertThat(table1Results)
                .containsExactly(
                        "default_namespace3.default_schema3.table3:col1=1;col2=1;col3=",
                        "default_namespace3.default_schema3.table3:col1=2;col2=2;col3=",
                        "default_namespace3.default_schema3.table3:col1=3;col2=3;col3=",
                        "default_namespace4.default_schema4.table4:col1=1;col2=1;col4=",
                        "default_namespace4.default_schema4.table4:col1=2;col2=2;col4=",
                        "default_namespace4.default_schema4.table4:col1=3;col2=3;col4=");

        String[] outputEvents = outCaptor.toString().trim().split(LINE_SEPARATOR);
        assertThat(outputEvents)
                .contains(
                        "CreateTableEvent{tableId=default_namespace3.default_schema3.table3, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace3.default_schema3.table3, before=[], after=[1, 1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace3.default_schema3.table3, before=[], after=[2, 2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace3.default_schema3.table3, before=[], after=[3, 3, null], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace3.default_schema3.table3, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                        "CreateTableEvent{tableId=default_namespace4.default_schema4.table4, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                        "DataChangeEvent{tableId=default_namespace4.default_schema4.table4, before=[], after=[1, 1, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace4.default_schema4.table4, before=[], after=[2, 2, null], op=INSERT, meta=()}",
                        "DataChangeEvent{tableId=default_namespace4.default_schema4.table4, before=[], after=[3, 3, null], op=INSERT, meta=()}",
                        "AddColumnEvent{tableId=default_namespace4.default_schema4.table4, addedColumns=[ColumnWithPosition{column=`col4` STRING, position=LAST, existedColumnName=null}]}");
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
