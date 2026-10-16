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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSink;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkFactory;
import org.apache.flink.cdc.runtime.operators.StreamNodeAdapter;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.util.Collector;

import org.apache.iceberg.flink.maintenance.operator.TriggerManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaintenanceGraphAdapterTest {
    @TempDir Path directory;

    @Test
    void preservesNativeGraphContractsAndOnlyAdaptsOwnedOperators() {
        String compatibility =
                org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("2.")
                        ? "flink2-compat"
                        : "flink1-compat";
        assertThat(
                        StreamNodeAdapter.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toString())
                .contains(compatibility);
        StreamExecutionEnvironment env = environment();
        env.fromSequence(1, 1).map(value -> value).uid("unrelated");
        MaintenanceGraphAdapter adapter = append(env);
        StreamGraph graph = env.getStreamGraph();
        Map<String, StreamOperatorFactory<?>> originals = factories(graph);
        Map<String, String> contracts = contracts(graph);

        adapter.postProcessStreamGraph(graph);

        assertThat(contracts(graph)).isEqualTo(contracts);
        assertThat(factories(graph).get("unrelated")).isSameAs(originals.get("unrelated"));
        List<Object> functions = new ArrayList<>();
        for (StreamNode node : graph.getStreamNodes()) {
            if (node.getOperatorFactory() instanceof SimpleOperatorFactory) {
                Object operator =
                        ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
                if (operator instanceof AbstractUdfStreamOperator) {
                    Object function =
                            ((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction();
                    functions.add(function);
                    if (function instanceof TriggerManager) {
                        assertThat(node.getOperatorFactory())
                                .isSameAs(originals.get(node.getTransformationUID()));
                    }
                }
            }
        }
        assertThat(functions.stream().filter(DeferredProcessFunction.class::isInstance).count())
                .isEqualTo(14);
        Map<String, StreamOperatorFactory<?>> adapted = factories(graph);
        adapter.postProcessStreamGraph(graph);
        assertThat(factories(graph)).isEqualTo(adapted);
        assertThat(directory.resolve("sales/orders/metadata")).doesNotExist();
    }

    @Test
    void rebuildsTheSinkGraphAfterExecutionPlanInspection() {
        Map<String, String> values = MaintenanceOptionsTest.validOptions();
        values.put("catalog.properties.type", "hadoop");
        values.put("catalog.properties.warehouse", directory.toString());
        values.put("sink.maintenance.rewrite-data-files.enabled", "true");
        IcebergDataSink sink =
                (IcebergDataSink)
                        new IcebergDataSinkFactory()
                                .createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                Configuration.fromMap(values),
                                                new Configuration(),
                                                getClass().getClassLoader()));
        StreamExecutionEnvironment env = environment();
        env.fromCollection(Collections.emptyList(), new EventTypeInfo())
                .uid("cdc-input")
                .sinkTo(((FlinkSinkProvider) sink.getEventSinkProvider()).getSink())
                .uid("sink: nested");

        StreamGraph preview = env.getStreamGraph(false);
        sink.postProcessStreamGraph(preview);
        StreamGraph submitted = env.getStreamGraph();
        sink.postProcessStreamGraph(submitted);

        assertThat(factories(submitted).keySet()).isEqualTo(factories(preview).keySet());
        assertThat(directory.resolve("sales/orders/metadata")).doesNotExist();
    }

    @Test
    void rejectsAnUnexpectedNativeFunctionBeforeSubmission() {
        StreamExecutionEnvironment env = environment();
        MaintenanceGraphAdapter adapter = append(env);
        StreamGraph graph = env.getStreamGraph();
        StreamNode planner =
                graph.getStreamNodes().stream()
                        .filter(node -> node.getOperatorName().startsWith("RDF Planner"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        StreamNodeAdapter.setOperatorFactory(
                planner, SimpleOperatorFactory.of(new ProcessOperator<>(new PassThrough())));

        assertThatThrownBy(() -> adapter.postProcessStreamGraph(graph))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Iceberg maintenance operator changed");
    }

    @Test
    void rejectsAnAdditionalOperatorInANativeTask() {
        StreamExecutionEnvironment env = environment();
        MaintenanceGraphAdapter adapter = append(env);
        StreamGraph graph = env.getStreamGraph(false);
        String plannerUid =
                graph.getStreamNodes().stream()
                        .map(StreamNode::getTransformationUID)
                        .filter(uid -> uid != null && uid.startsWith("RDF Planner"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        // A new Iceberg function might also read metadata during open(). Require revalidation.
        env.fromSequence(1, 1)
                .map(value -> value)
                .uid("New operator" + plannerUid.substring("RDF Planner".length()));
        StreamGraph changed = env.getStreamGraph();

        assertThatThrownBy(() -> adapter.postProcessStreamGraph(changed))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected native maintenance operator");
    }

    private MaintenanceGraphAdapter append(StreamExecutionEnvironment env) {
        Map<String, String> values = MaintenanceOptionsTest.validOptions();
        values.put("sink.maintenance.tables", "sales.orders;sales.users");
        values.put("sink.maintenance.rewrite-data-files.enabled", "true");
        values.put("sink.maintenance.delete-orphan-files.enabled", "true");
        Map<String, String> catalog = new HashMap<>();
        catalog.put("type", "hadoop");
        catalog.put("warehouse", directory.toString());
        return TableMaintenanceTopology.append(
                env,
                catalog,
                Collections.emptyMap(),
                MaintenanceOptions.fromConfiguration(Configuration.fromMap(values)));
    }

    private static StreamExecutionEnvironment environment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        return env;
    }

    private static Map<String, StreamOperatorFactory<?>> factories(StreamGraph graph) {
        Map<String, StreamOperatorFactory<?>> factories = new HashMap<>();
        graph.getStreamNodes()
                .forEach(
                        node ->
                                factories.put(
                                        node.getTransformationUID(), node.getOperatorFactory()));
        return factories;
    }

    private static Map<String, String> contracts(StreamGraph graph) {
        return graph.getStreamNodes().stream()
                .collect(
                        Collectors.toMap(
                                StreamNode::getTransformationUID,
                                node ->
                                        node.getOperatorName()
                                                + ":"
                                                + node.getParallelism()
                                                + ":"
                                                + node.getSlotSharingGroup()
                                                + ":"
                                                + node.getInEdges()
                                                + ":"
                                                + node.getOutEdges()
                                                + ":"
                                                + Arrays.toString(node.getTypeSerializersIn())
                                                + ":"
                                                + node.getTypeSerializerOut()));
    }

    private static class PassThrough extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context context, Collector<String> output) {
            output.collect(value);
        }
    }
}
