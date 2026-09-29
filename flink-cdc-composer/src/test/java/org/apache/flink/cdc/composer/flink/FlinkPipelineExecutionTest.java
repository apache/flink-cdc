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

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.common.sink.SupportsStreamGraphPostProcessing;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkPipelineExecutionTest {

    @Test
    void adaptsTheExpandedGraphBeforeSubmittingItAndWaitsWhenBlocking() throws Exception {
        RecordingEnvironment env = new RecordingEnvironment();
        env.fromSequence(1, 1).print();
        AtomicReference<StreamGraph> adapted = new AtomicReference<>();
        PipelineExecution.ExecutionInfo result =
                new FlinkPipelineExecution(
                                env,
                                "pipeline",
                                true,
                                graph -> {
                                    env.calls.add("adapt");
                                    assertThat(graph.getJobName()).isEqualTo("pipeline");
                                    assertThat(graph.getStreamNodes()).isNotEmpty();
                                    adapted.set(graph);
                                })
                        .execute();

        assertThat(env.calls).containsExactly("graph", "adapt", "submit", "wait");
        assertThat(env.submitted).isSameAs(adapted.get());
        assertThat(result.getId()).isEqualTo(env.id.toString());
        assertThat(result.getDescription()).isEqualTo("pipeline");
    }

    @Test
    void doesNotSubmitIfGraphAdaptationFails() {
        RecordingEnvironment env = new RecordingEnvironment();
        env.fromSequence(1, 1).print();
        assertThatThrownBy(
                        () ->
                                new FlinkPipelineExecution(
                                                env,
                                                "pipeline",
                                                false,
                                                graph -> {
                                                    throw new IllegalStateException(
                                                            "Incompatible operator");
                                                })
                                        .execute())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Incompatible operator");
        assertThat(env.calls).containsExactly("graph");
    }

    @Test
    void preservesExecutionWhenTheSinkOptsOutOfGraphPostProcessing() throws Exception {
        RecordingEnvironment env = new RecordingEnvironment();
        SupportsStreamGraphPostProcessing processor =
                new SupportsStreamGraphPostProcessing() {
                    @Override
                    public boolean requiresStreamGraphPostProcessing() {
                        return false;
                    }

                    @Override
                    public void postProcessStreamGraph(StreamGraph graph) {
                        throw new AssertionError("Disabled graph post-processing must not run");
                    }
                };
        new FlinkPipelineExecution(env, "pipeline", false, processor).execute();
        assertThat(env.calls).containsExactly("legacy:pipeline");
    }

    @Test
    void preservesExecutionForSinksWithoutGraphPostProcessing() throws Exception {
        RecordingEnvironment env = new RecordingEnvironment();
        new FlinkPipelineExecution(env, "pipeline", false).execute();
        assertThat(env.calls).containsExactly("legacy:pipeline");
    }

    static class RecordingEnvironment extends StreamExecutionEnvironment {
        final List<String> calls = new ArrayList<>();
        final JobID id = new JobID();
        StreamGraph submitted;
        private final JobClient client =
                (JobClient)
                        Proxy.newProxyInstance(
                                JobClient.class.getClassLoader(),
                                new Class<?>[] {JobClient.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "getJobID":
                                            return id;
                                        case "getJobExecutionResult":
                                            calls.add("wait");
                                            return CompletableFuture.completedFuture(null);
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });

        @Override
        public StreamGraph getStreamGraph() {
            calls.add("graph");
            return super.getStreamGraph();
        }

        @Override
        public JobClient executeAsync(StreamGraph graph) {
            calls.add("submit");
            submitted = graph;
            return client;
        }

        @Override
        public JobClient executeAsync(String name) {
            calls.add("legacy:" + name);
            return client;
        }
    }
}
