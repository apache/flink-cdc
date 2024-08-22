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
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
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

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

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
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.singletonList(
                                new TransformDef(
                                        tableId.toString(), "*", null, null, null, null, null)),
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
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`extend_id` STRING}, primaryKeys=id, options=()}",
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
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT,`name` STRING,`age` INT,`extend_id` STRING}, primaryKeys=id, options=()}",
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
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`extend_id` STRING,`id` INT,`name` STRING,`age` INT}, primaryKeys=id, options=()}",
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

        assertThat(instant.toEpochMilli() % milliSecondsInOneDay)
                .isEqualTo(Long.parseLong(localTime));

        String currentDate = tokens.get(5);

        assertThat(instant.toEpochMilli() / milliSecondsInOneDay)
                .isEqualTo(Long.parseLong(currentDate));
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
