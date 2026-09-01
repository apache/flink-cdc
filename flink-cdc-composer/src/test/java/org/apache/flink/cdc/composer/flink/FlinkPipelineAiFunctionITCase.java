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
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.cdc.models.dummy.DummyModelClient;
import org.apache.flink.cdc.models.dummy.DummyModelClientFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.apache.flink.configuration.PipelineOptions.JARS;
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

    @TempDir Path tempDir;

    @BeforeEach
    void init() {
        System.setOut(new PrintStream(outCaptor));
        ValuesDatabase.clear();
    }

    @AfterEach
    void cleanup() {
        System.setOut(standardOut);
    }

    @Test
    void testAiCompleteInProjection() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, content, AI_COMPLETE('testModel', content, 'Complete the text') AS completed",
                        List.of(
                                ModelDef.of(
                                        "testModel",
                                        "dummy",
                                        Collections.singletonMap("debug", "true"))));
        assertThat(output)
                .contains(
                        "Dummy model opened.",
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`content` STRING,`completed` VARIANT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, I love this product, {\"result\":\"dummy response\"}], op=INSERT, meta=()}",
                        "Dummy model closed.");
    }

    @Test
    void testSpecializedTextAiFunctionsInProjection() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, "
                                + "AI_CLASSIFY('testModel', content, 'positive,negative') AS classified, "
                                + "AI_TRANSLATE('testModel', content, 'auto', 'en') AS translated, "
                                + "AI_SUMMARIZE('testModel', content, 100) AS summarized, "
                                + "AI_SENTIMENT('testModel', content) AS sentiment, "
                                + "AI_EXTRACT('testModel', content, 'name:string') AS extracted, "
                                + "AI_MASK('testModel', content, 'name') AS masked",
                        List.of(ModelDef.of("testModel", "dummy", Collections.emptyMap())));

        assertThat(output)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`classified` VARIANT,`translated` VARIANT,`summarized` VARIANT,`sentiment` VARIANT,`extracted` VARIANT,`masked` VARIANT}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, {\"category\":\"dummy\",\"confidence\":1}, {\"detected_language\":\"en\",\"translated_text\":\"dummy translation\"}, {\"summary\":\"dummy summary\"}, {\"confidence\":1,\"label\":\"neutral\",\"score\":0}, {\"extracted_json\":{\"name\":\"dummy\"}}, {\"detected_entities\":\"name\",\"masked_text\":\"d***y\"}], op=INSERT, meta=()}");
    }

    @Test
    void testSameNamedUdfTakesPrecedenceOverAiFunction() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, AI_SENTIMENT(id) AS sentiment",
                        List.of(ModelDef.of("unusedModel", "dummy", Collections.emptyMap())),
                        List.of(
                                new UdfDef(
                                        "ai_sentiment",
                                        "org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass")));

        assertThat(output)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`sentiment` STRING}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, 2], op=INSERT, meta=()}");
    }

    @Test
    void testAiEmbedInProjection() throws Exception {
        String[] output =
                runAiFunctionTest(
                        "id, AI_EMBED('embedModel', content) AS embedding",
                        List.of(ModelDef.of("embedModel", "dummy", Collections.emptyMap())));
        assertThat(output)
                .containsExactly(
                        "CreateTableEvent{tableId=default_namespace.default_schema.mytable1, schema=columns={`id` INT NOT NULL,`embedding` ARRAY<FLOAT>}, primaryKeys=id, options=()}",
                        "DataChangeEvent{tableId=default_namespace.default_schema.mytable1, before=[], after=[1, [3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]], op=INSERT, meta=()}");
    }

    private String[] runAiFunctionTest(String projection, List<ModelDef> models) throws Exception {
        return runAiFunctionTest(projection, models, Collections.emptyList());
    }

    private String[] runAiFunctionTest(
            String projection, List<ModelDef> models, List<UdfDef> udfFunctions) throws Exception {
        URL modelJar = createDummyModelJar().toUri().toURL();
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader modelClassLoader =
                new DummyModelClassLoader(modelJar, originalClassLoader)) {
            Thread.currentThread().setContextClassLoader(modelClassLoader);
            return runAiFunctionTest(projection, models, udfFunctions, modelJar);
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private String[] runAiFunctionTest(
            String projection, List<ModelDef> models, List<UdfDef> udfFunctions, URL modelJar)
            throws Exception {
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
                        udfFunctions,
                        models,
                        pipelineConfig);

        // Execute & capture output
        PipelineExecution execution = composer.compose(pipelineDef);
        assertThat(composer.getEnv().getConfiguration().get(JARS))
                .as("AI model provider JARs uploaded with the JobGraph")
                .contains(modelJar.toString());
        execution.execute();

        return outCaptor.toString().trim().split("\n");
    }

    private Path createDummyModelJar() throws IOException {
        Path modelJar = tempDir.resolve("dummy-model.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(modelJar))) {
            addClassToJar(DummyModelClient.class, output);
            addClassToJar(DummyModelClientFactory.class, output);

            output.putNextEntry(
                    new JarEntry(
                            "META-INF/services/org.apache.flink.cdc.common.factories.Factory"));
            output.write(
                    (DummyModelClientFactory.class.getName() + "\n")
                            .getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return modelJar;
    }

    private static void addClassToJar(Class<?> clazz, JarOutputStream output) throws IOException {
        String resourceName = clazz.getName().replace('.', '/') + ".class";
        try (InputStream input = clazz.getClassLoader().getResourceAsStream(resourceName)) {
            assertThat(input).as("class resource %s", resourceName).isNotNull();
            output.putNextEntry(new JarEntry(resourceName));
            input.transferTo(output);
            output.closeEntry();
        }
    }

    private static final class DummyModelClassLoader extends URLClassLoader {

        private static final String DUMMY_MODEL_PACKAGE = "org.apache.flink.cdc.models.dummy.";

        private DummyModelClassLoader(URL modelJar, ClassLoader parent) {
            super(new URL[] {modelJar}, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith(DUMMY_MODEL_PACKAGE)) {
                return super.loadClass(name, resolve);
            }

            synchronized (getClassLoadingLock(name)) {
                Class<?> clazz = findLoadedClass(name);
                if (clazz == null) {
                    try {
                        clazz = findClass(name);
                    } catch (ClassNotFoundException e) {
                        clazz = super.loadClass(name, false);
                    }
                }
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        }
    }
}
