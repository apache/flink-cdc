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

package org.apache.flink.cdc.composer.specs;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.test.parameters.ParameterProperty;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.apache.flink.cdc.common.types.DataTypes.ARRAY;
import static org.apache.flink.cdc.common.types.DataTypes.BIGINT;
import static org.apache.flink.cdc.common.types.DataTypes.BINARY;
import static org.apache.flink.cdc.common.types.DataTypes.BOOLEAN;
import static org.apache.flink.cdc.common.types.DataTypes.BYTES;
import static org.apache.flink.cdc.common.types.DataTypes.CHAR;
import static org.apache.flink.cdc.common.types.DataTypes.DATE;
import static org.apache.flink.cdc.common.types.DataTypes.DECIMAL;
import static org.apache.flink.cdc.common.types.DataTypes.DOUBLE;
import static org.apache.flink.cdc.common.types.DataTypes.FIELD;
import static org.apache.flink.cdc.common.types.DataTypes.FLOAT;
import static org.apache.flink.cdc.common.types.DataTypes.INT;
import static org.apache.flink.cdc.common.types.DataTypes.MAP;
import static org.apache.flink.cdc.common.types.DataTypes.ROW;
import static org.apache.flink.cdc.common.types.DataTypes.SMALLINT;
import static org.apache.flink.cdc.common.types.DataTypes.STRING;
import static org.apache.flink.cdc.common.types.DataTypes.TIME;
import static org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP;
import static org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_TZ;
import static org.apache.flink.cdc.common.types.DataTypes.TINYINT;
import static org.apache.flink.cdc.common.types.DataTypes.VARBINARY;
import static org.apache.flink.cdc.common.types.DataTypes.VARCHAR;
import static org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory.IDENTIFIER;
import static org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions.PRINT_ENABLED;
import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS;
import static org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions.EVENT_SET_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** Spec based transform module IT cases. */
class TransformSpecsITCase {

    private static final TableId testTableId = TableId.tableId("foo", "bar", "baz");
    private static final Schema testInputSchema =
            Schema.newBuilder()
                    .physicalColumn("id_", BIGINT().notNull(), "Identifier")
                    .physicalColumn("bool_", BOOLEAN(), "George", "false")
                    .physicalColumn("tinyint_", TINYINT())
                    .physicalColumn("smallint_", SMALLINT())
                    .physicalColumn("int_", INT())
                    .physicalColumn("bigint_", BIGINT())
                    .physicalColumn("float_", FLOAT())
                    .physicalColumn("double_", DOUBLE())
                    .physicalColumn("decimal_10_0_", DECIMAL(10, 0))
                    .physicalColumn("decimal_20_2_", DECIMAL(20, 2))
                    .physicalColumn("char_", CHAR(140), "Let's Tweet", "...")
                    .physicalColumn("varchar_", VARCHAR(140))
                    .physicalColumn("string_", STRING())
                    .physicalColumn("binary_", BINARY(640))
                    .physicalColumn("varbinary_", VARBINARY(640))
                    .physicalColumn("bytes_", BYTES())
                    .physicalColumn("timestamp_0_", TIMESTAMP(0))
                    .physicalColumn("timestamp_6_", TIMESTAMP(6))
                    .physicalColumn("timestamp_9_", TIMESTAMP(9))
                    .physicalColumn("timestamp_tz_0_", TIMESTAMP_TZ(0))
                    .physicalColumn("timestamp_tz_6_", TIMESTAMP_TZ(6))
                    .physicalColumn("timestamp_tz_9_", TIMESTAMP_TZ(9))
                    .physicalColumn("timestamp_ltz_0_", TIMESTAMP_LTZ(0))
                    .physicalColumn("timestamp_ltz_6_", TIMESTAMP_LTZ(6))
                    .physicalColumn("timestamp_ltz_9_", TIMESTAMP_LTZ(9))
                    .physicalColumn("date_", DATE())
                    .physicalColumn("time_0_", TIME(0))
                    .physicalColumn("time_6_", TIME(6))
                    .physicalColumn("time_9_", TIME(9))
                    .physicalColumn("array_int_", ARRAY(INT()))
                    .physicalColumn("array_string_", ARRAY(STRING()))
                    .physicalColumn("map_int_string_", MAP(INT(), STRING()))
                    .physicalColumn("map_string_array_string_", MAP(STRING(), ARRAY(STRING())))
                    .physicalColumn(
                            "complex_row_", ROW(FIELD("name", STRING()), FIELD("length", INT())))
                    .build();

    private static final BinaryRecordDataGenerator generator =
            new BinaryRecordDataGenerator(
                    testInputSchema.getColumnDataTypes().toArray(new DataType[0]));
    private static final List<Event> testInputSuites = new ArrayList<>();
    private static final List<Event> nonNullTestInputSuites = new ArrayList<>();

    static {
        BinaryRecordDataGenerator nestedGenerator =
                new BinaryRecordDataGenerator(new DataType[] {STRING(), INT()});

        BinaryRecordData record1 =
                generator.generate(
                        new Object[] {
                            1L,
                            true,
                            (byte) 2,
                            (short) 3,
                            4,
                            5L,
                            7.7f,
                            88.88d,
                            DecimalData.fromBigDecimal(new BigDecimal("1234567890"), 10, 0),
                            DecimalData.fromBigDecimal(
                                    new BigDecimal("123456789012345678.90"), 20, 2),
                            BinaryStringData.fromString("Alice"),
                            BinaryStringData.fromString("Zorro"),
                            BinaryStringData.fromString("From A to Z is Lie"),
                            "Lorem ipsum".getBytes(StandardCharsets.UTF_8),
                            "dolor sit amet".getBytes(StandardCharsets.UTF_8),
                            "amet consectetuer".getBytes(StandardCharsets.UTF_8),
                            TimestampData.fromMillis(123456789, 123456),
                            TimestampData.fromMillis(234567891, 234561),
                            TimestampData.fromMillis(345678912, 345612),
                            ZonedTimestampData.of(123456789, 123456, "Asia/Shanghai"),
                            ZonedTimestampData.of(234567891, 234561, "Europe/Berlin"),
                            ZonedTimestampData.of(345678912, 345612, "America/Puerto_Rico"),
                            LocalZonedTimestampData.fromEpochMillis(123456789, 123456),
                            LocalZonedTimestampData.fromEpochMillis(234567891, 234561),
                            LocalZonedTimestampData.fromEpochMillis(345678912, 345612),
                            DateData.fromLocalDate(LocalDate.of(2000, 12, 31)),
                            TimeData.fromLocalTime(LocalTime.of(19, 43, 17)),
                            TimeData.fromLocalTime(LocalTime.of(21, 45, 3)),
                            TimeData.fromLocalTime(LocalTime.of(3, 59, 59)),
                            new GenericArrayData(new int[] {1, 1, 2, 3, 5, 8, 13}),
                            new GenericArrayData(
                                    new Object[] {
                                        s("one"), s("one"), s("two"), s("three"), s("five")
                                    }),
                            m(1, s("one"), 2, s("two"), 3, s("three")),
                            m(
                                    s("one"),
                                    new GenericArrayData(new Object[] {s("O"), s("N"), s("E")}),
                                    s("two"),
                                    new GenericArrayData(new Object[] {s("T"), s("W"), s("O")}),
                                    s("three"),
                                    new GenericArrayData(
                                            new Object[] {s("T"), s("H"), s("R"), s("E"), s("E")})),
                            nestedGenerator.generate(new Object[] {s("Alice"), 5})
                        });

        BinaryRecordData record2 =
                generator.generate(
                        new Object[] {
                            -1L,
                            false,
                            (byte) -2,
                            (short) -3,
                            -4,
                            -5L,
                            -7.7f,
                            -88.88d,
                            DecimalData.fromBigDecimal(new BigDecimal("-9876543210"), 10, 0),
                            DecimalData.fromBigDecimal(
                                    new BigDecimal("-987654321098765432.10"), 20, 2),
                            BinaryStringData.fromString("爱丽丝"),
                            BinaryStringData.fromString("疯帽子"),
                            BinaryStringData.fromString("天地玄黄宇宙洪荒"),
                            "一二三四五".getBytes(StandardCharsets.UTF_8),
                            "六七八九十".getBytes(StandardCharsets.UTF_8),
                            "吾輩は猫である".getBytes(StandardCharsets.UTF_8),
                            TimestampData.fromMillis(723456789, 723456),
                            TimestampData.fromMillis(834567891, 834561),
                            TimestampData.fromMillis(945678912, 945612),
                            ZonedTimestampData.of(723456789, 723456, "Asia/Shanghai"),
                            ZonedTimestampData.of(834567891, 834561, "Europe/Berlin"),
                            ZonedTimestampData.of(945678912, 945612, "America/Puerto_Rico"),
                            LocalZonedTimestampData.fromEpochMillis(723456789, 723456),
                            LocalZonedTimestampData.fromEpochMillis(834567891, 834561),
                            LocalZonedTimestampData.fromEpochMillis(945678912, 945612),
                            DateData.fromLocalDate(LocalDate.of(2001, 1, 1)),
                            TimeData.fromLocalTime(LocalTime.of(12, 34, 45)),
                            TimeData.fromLocalTime(LocalTime.of(23, 45, 7)),
                            TimeData.fromLocalTime(LocalTime.of(2, 30, 5)),
                            new GenericArrayData(new int[] {2, 3, 5, 7, 11, 13, 17, 19}),
                            new GenericArrayData(
                                    new Object[] {s("二"), s("san"), s("五"), s("qi"), s("十一")}),
                            m(1, s("yi"), 2, s("er"), 3, s("san")),
                            m(
                                    s("一"),
                                    new GenericArrayData(new Object[] {s("Y"), s("I")}),
                                    s("二"),
                                    new GenericArrayData(new Object[] {s("E"), s("R")}),
                                    s("三"),
                                    new GenericArrayData(new Object[] {s("S"), s("A"), s("N")})),
                            nestedGenerator.generate(new Object[] {s("Derrida"), 7})
                        });

        BinaryRecordData record3 =
                generator.generate(
                        new Object[] {
                            0L, null, null, null, null, null, null, null, null, null, null, null,
                            null, null, null, null, null, null, null, null, null, null, null, null,
                            null, null, null, null, null, null, null, null, null, null
                        });

        testInputSuites.add(new CreateTableEvent(testTableId, testInputSchema));
        testInputSuites.add(DataChangeEvent.insertEvent(testTableId, record1));
        testInputSuites.add(DataChangeEvent.updateEvent(testTableId, record1, record2));
        testInputSuites.add(DataChangeEvent.deleteEvent(testTableId, record2));
        testInputSuites.add(DataChangeEvent.insertEvent(testTableId, record3));
        testInputSuites.add(DataChangeEvent.deleteEvent(testTableId, record3));

        nonNullTestInputSuites.add(new CreateTableEvent(testTableId, testInputSchema));
        nonNullTestInputSuites.add(DataChangeEvent.insertEvent(testTableId, record1));
        nonNullTestInputSuites.add(DataChangeEvent.updateEvent(testTableId, record1, record2));
        nonNullTestInputSuites.add(DataChangeEvent.deleteEvent(testTableId, record2));
    }

    private static BinaryStringData s(Object wrappedString) {
        return BinaryStringData.fromString(Objects.toString(wrappedString));
    }

    private static <K, V> GenericMapData m(K k1, V v1, K k2, V v2, K k3, V v3) {
        Map<K, V> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return new GenericMapData(map);
    }

    private static final ParameterProperty<Path> MODULE_DIRECTORY =
            new ParameterProperty<>("moduleDir", Paths::get);

    static Stream<Arguments> loadTestSpecs() throws IOException {
        Path path =
                MODULE_DIRECTORY
                        .get(Paths.get("").toAbsolutePath())
                        .resolve("src/test/resources/specs");

        try (Stream<Path> dependencyResources = Files.walk(path)) {
            List<Path> p = dependencyResources.sorted().collect(Collectors.toList());
            return p.stream()
                    .filter(f -> f.getFileName().toString().endsWith(".yaml"))
                    .flatMap(TransformSpecsITCase::loadTestSpec)
                    .map(spec -> Arguments.of(spec.group, spec.name, spec));
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @SuppressWarnings("unchecked")
    private static Stream<TestSpec> loadTestSpec(Path specPath) {
        List<TestSpec> specs = new ArrayList<>();
        try {
            for (Iterator<JsonNode> it = mapper.readTree(specPath.toFile()).elements();
                    it.hasNext(); ) {
                JsonNode specNode = it.next();
                TestSpec spec = new TestSpec();
                spec.group = specPath.getFileName().toString();
                spec.name = asTextOrNull(specNode.get("do"));
                spec.ignore = asTextOrNull(specNode.get("ignore"));
                if (specNode.has("time-zone")) {
                    spec.timeZone = asTextOrNull(specNode.get("time-zone"));
                }
                if (specNode.has("projection")) {
                    spec.projectionRules =
                            List.of(
                                    specNode.get("projection")
                                            .asText()
                                            .split(System.lineSeparator()));
                }
                spec.filterRule = asTextOrNull(specNode.get("filter"));
                spec.primaryKey = asTextOrNull(specNode.get("primary-key"));
                spec.partitionKey = asTextOrNull(specNode.get("partition-key"));
                spec.tableOptions = asTextOrNull(specNode.get("table-options"));
                spec.postConverters = asTextOrNull(specNode.get("converters"));
                if (specNode.has("expect")) {
                    spec.expectedLines =
                            List.of(specNode.get("expect").asText().split(System.lineSeparator()));
                }
                if (specNode.has("expect-error")) {
                    spec.expectedErrors =
                            List.of(
                                    specNode.get("expect-error")
                                            .asText()
                                            .split(System.lineSeparator()));
                }
                if (specNode.has("non-null")) {
                    spec.nonNull = specNode.get("non-null").asBoolean();
                }
                specs.add(spec);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to laod test spec file: " + specPath, e);
        }
        return specs.stream();
    }

    static String asTextOrNull(JsonNode node) {
        return node == null ? null : node.asText();
    }

    static class TestSpec {
        public String group;
        public String name;
        public String ignore;
        public String timeZone = "UTC";
        public List<String> projectionRules = new ArrayList<>();
        public @Nullable String filterRule;
        public @Nullable String primaryKey;
        public @Nullable String partitionKey;
        public @Nullable String tableOptions;
        public @Nullable String postConverters;
        public List<String> expectedLines = new ArrayList<>();
        public List<String> expectedErrors = new ArrayList<>();
        public boolean nonNull = false;
    }

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void takeOverStdOut() {
        System.setOut(new PrintStream(outCaptor));
    }

    @AfterEach
    void handInStdOut() {
        System.setOut(standardOut);
        outCaptor.reset();
    }

    @Test
    void testRules() throws Exception {
        List<String> loadedRules =
                loadTestSpecs()
                        .map(Arguments::get)
                        .map(arr -> "specs/" + arr[0])
                        .distinct()
                        .sorted()
                        .collect(Collectors.toList());

        assertThat(loadedRules).containsExactly(EXPECTED_SPECS);
    }

    @ParameterizedTest(name = "{0} :: {1}")
    @MethodSource("loadTestSpecs")
    void runTransformSpecs(String group, String name, TestSpec spec) throws Exception {
        Assumptions.assumeThat(spec.ignore)
                .as("Test case %s is ignored until we close %s", spec.name, spec.ignore)
                .isNull();
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(EVENT_SET_ID, CUSTOM_SOURCE_EVENTS);
        SourceDef sourceDef = new SourceDef(IDENTIFIER, "Value Source", sourceConfig);

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(PRINT_ENABLED, true);
        SinkDef sinkDef = new SinkDef(IDENTIFIER, "Value Sink", sinkConfig);

        ValuesDataSourceHelper.setSourceEvents(
                singletonList(spec.nonNull ? nonNullTestInputSuites : testInputSuites));

        TransformDef transformDef =
                new TransformDef(
                        testTableId.toString(),
                        spec.projectionRules.isEmpty()
                                ? null
                                : String.join(", ", spec.projectionRules),
                        spec.filterRule,
                        spec.primaryKey,
                        spec.partitionKey,
                        spec.tableOptions,
                        String.format("%s :: %s", group, name),
                        spec.postConverters);

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, spec.timeZone);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.EVOLVE);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        singletonList(transformDef),
                        Collections.emptyList(),
                        pipelineConfig);

        PipelineExecution execution = composer.compose(pipelineDef);

        if (!spec.expectedErrors.isEmpty()) {
            StringWriter sw = new StringWriter();
            try {
                execution.execute();
                fail("No exceptions thrown while we're expecting one.");
            } catch (Exception e) {
                e.printStackTrace(new PrintWriter(sw));
            }
            assertThat(sw.toString()).contains(spec.expectedErrors);
        } else {
            execution.execute();
            String[] outputEvents = outCaptor.toString().trim().split(System.lineSeparator());
            assertThat(outputEvents).containsExactlyElementsOf(spec.expectedLines);
        }
    }

    enum SpecContext {
        PROJECTION,
        EXPECT,
        EXPECT_ERROR,
        NULL
    }

    private static final String[] EXPECTED_SPECS = {
        "specs/arithmetic.yaml",
        "specs/basic.yaml",
        "specs/casting.yaml",
        "specs/comparison.yaml",
        "specs/condition.yaml",
        "specs/decimal.yaml",
        "specs/logical.yaml",
        "specs/meta.yaml",
        "specs/nested.yaml",
        "specs/string.yaml",
        "specs/temporal.yaml"
    };
}
