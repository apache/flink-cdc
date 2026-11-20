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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.flink.coordination.OperatorIDGenerator;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.cdc.runtime.typeutils.PartitioningEventTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Integration tests for {@link SchemaOperatorTranslator}. */
@Timeout(120)
class SchemaOperatorTranslatorITCase {

    private static final Queue<ChangeEvent> OUTPUT_EVENTS = new ConcurrentLinkedQueue<>();

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .setConfiguration(clusterConfiguration())
                            .build());

    private static org.apache.flink.configuration.Configuration clusterConfiguration() {
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration();
        configuration.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
        return configuration;
    }

    @BeforeEach
    void clearResults() {
        OUTPUT_EVENTS.clear();
        ValuesDatabase.clear();
    }

    private static Stream<Arguments> routingModes() {
        return Stream.of("streaming", "batch", "distributed")
                .flatMap(
                        topology ->
                                Stream.of("symbol", "capture", "escaped-capture")
                                        .map(replacement -> Arguments.of(topology, replacement)));
    }

    @ParameterizedTest
    @MethodSource("routingModes")
    void testRoutedSchemaAndDataEvents(String topology, String replacement) throws Exception {
        boolean useReplacementSymbol = replacement.equals("symbol");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        boolean batch = topology.equals("batch");
        env.setRuntimeMode(batch ? RuntimeExecutionMode.BATCH : RuntimeExecutionMode.STREAMING);

        TableId source = TableId.tableId("source_db", "daily.archive");
        Schema schema =
                Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));
        List<Event> events =
                Arrays.asList(
                        new CreateTableEvent(source, schema),
                        DataChangeEvent.insertEvent(source, generator.generate(new Object[] {1})));
        RouteDef route =
                new RouteDef(
                        useReplacementSymbol ? "source_db.\\.*" : "source_db.(\\.*)",
                        useReplacementSymbol
                                ? "target_db.<>"
                                : replacement.equals("escaped-capture")
                                        ? "target_db\\.$1"
                                        : "target_db.$1",
                        useReplacementSymbol ? "<>" : null,
                        null);
        ValuesDataSink sink =
                new ValuesDataSink(true, false, ValuesDataSink.SinkApi.SINK_V2, false);
        String schemaUid = "schema-routing";
        SchemaOperatorTranslator translator =
                new SchemaOperatorTranslator(
                        SchemaChangeBehavior.LENIENT, schemaUid, Duration.ofSeconds(30), "UTC");
        DataStream<Event> routed;
        if (topology.equals("distributed")) {
            List<PartitioningEvent> partitioned =
                    Arrays.asList(
                            PartitioningEvent.ofDistributed(events.get(0), 0, 0),
                            PartitioningEvent.ofDistributed(events.get(1), 0, 0));
            routed =
                    translator.translateDistributed(
                            env.fromCollection(partitioned, new PartitioningEventTypeInfo()),
                            1,
                            sink.getMetadataApplier(),
                            Collections.singletonList(route),
                            RouteMode.ALL_MATCH);
        } else {
            routed =
                    translator.translateRegular(
                            env.fromCollection(events, new EventTypeInfo()),
                            1,
                            batch,
                            sink.getMetadataApplier(),
                            Collections.singletonList(route));
        }
        DataStream<Event> captured = routed.map(new CaptureEvents(), new EventTypeInfo());
        new DataSinkTranslator()
                .translate(
                        new SinkDef(ValuesDataFactory.IDENTIFIER, "values", new Configuration()),
                        captured,
                        sink,
                        batch,
                        new OperatorIDGenerator(schemaUid).generate(),
                        new OperatorUidGenerator("route-test"));
        env.execute("route table ID components");

        TableId expected = TableId.tableId("target_db", "daily.archive");
        assertThat(OUTPUT_EVENTS)
                .containsExactly(
                        new CreateTableEvent(expected, schema),
                        DataChangeEvent.insertEvent(
                                expected, generator.generate(new Object[] {1})));
        assertThat(OUTPUT_EVENTS)
                .extracting(
                        event -> event.tableId().getNamespace(),
                        event -> event.tableId().getSchemaName(),
                        event -> event.tableId().getTableName())
                .containsExactly(
                        tuple(
                                expected.getNamespace(),
                                expected.getSchemaName(),
                                expected.getTableName()),
                        tuple(
                                expected.getNamespace(),
                                expected.getSchemaName(),
                                expected.getTableName()));
        assertThat(ValuesDatabase.getTableSchema(expected)).isEqualTo(schema);
        assertThat(ValuesDatabase.getResults(expected))
                .containsExactly("target_db.daily.archive:id=1");
    }

    private static class CaptureEvents implements MapFunction<Event, Event> {
        @Override
        public Event map(Event event) {
            if (event instanceof ChangeEvent) {
                OUTPUT_EVENTS.add((ChangeEvent) event);
            }
            return event;
        }
    }
}
