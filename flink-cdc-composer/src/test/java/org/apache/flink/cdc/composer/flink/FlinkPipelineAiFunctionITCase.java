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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for AI functions in the Flink pipeline. */
class FlinkPipelineAiFunctionITCase {

    private static final int MAX_PARALLELISM = 4;

    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

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
        System.setOut(new PrintStream(outCaptor));
        ValuesDatabase.clear();
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
    }

    private static final String DUMMY_JSON = "{\"result\":\"dummy result\",\"summary\":\"TL;DR\"}";

    @Test
    void testAiCompleteInProjection() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, content, AI_COMPLETE('testModel', content, 'Classify sentiment') AS res",
                        List.of(new ModelDef("testModel", "dummy", new HashMap<>())));
        assertThat(output)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`content` STRING,`res` VARIANT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, I love this product, "
                                + DUMMY_JSON
                                + "], op=INSERT, meta=()}");
    }

    @Test
    void testAiEmbedInProjection() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, AI_EMBED('embedModel', content) AS embedding",
                        List.of(new ModelDef("embedModel", "dummy", new HashMap<>())));
        assertThat(output)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`embedding` ARRAY<FLOAT>}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, [3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]], op=INSERT, meta=()}");
    }

    @Test
    void testMultipleAiFunctionsWithSameModel() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, AI_COMPLETE('myModel', content, 'Classify') AS cls, AI_SUMMARIZE('myModel', content, 100) AS summary",
                        List.of(new ModelDef("myModel", "dummy", new HashMap<>())));
        assertThat(output)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`cls` VARIANT,`summary` VARIANT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, "
                                + DUMMY_JSON
                                + ", "
                                + DUMMY_JSON
                                + "], op=INSERT, meta=()}");
    }

    private String[] runAiFunctionTest(String projection, List<ModelDef> models) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Source: one table with a single row
        TableId tableId = TableId.tableId("default_namespace", "default_schema", "mytable1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("content", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        List<Event> events = new ArrayList<>();
        events.add(new CreateTableEvent(tableId, schema));
        events.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    1, BinaryStringData.fromString("I love this product")
                                })));
        ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

        // Sink
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

        // Transform
        TransformDef transformDef =
                new TransformDef(
                        "default_namespace.default_schema.mytable1",
                        projection,
                        null,
                        "id",
                        null,
                        null,
                        null,
                        null);

        // Pipeline
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
                        models,
                        pipelineConfig);

        // Execute & capture output
        PipelineExecution execution = composer.compose(pipelineDef);
        execution.execute();

        return outCaptor.toString().trim().split("\n");
    }
}
