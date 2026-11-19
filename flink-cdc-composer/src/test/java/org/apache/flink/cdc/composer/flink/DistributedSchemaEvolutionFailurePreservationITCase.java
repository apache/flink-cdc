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
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.testsink.factory.FailingDataSinkFactory;
import org.apache.flink.cdc.composer.testsource.factory.DistributedDataSourceFactory;
import org.apache.flink.cdc.composer.testsource.source.DistributedSourceOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;

/** Integration tests for preserving failures during distributed schema evolution. */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DistributedSchemaEvolutionFailurePreservationITCase {

    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(30);

    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
        MINI_CLUSTER_CONFIG.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        MINI_CLUSTER_CONFIG.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        MINI_CLUSTER_CONFIG.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    @BeforeEach
    void setup() {
        FailingDataSinkFactory.reset();
    }

    @AfterEach
    void cleanup() {
        FailingDataSinkFactory.releaseMetadataApplier();
    }

    @Test
    void testPreserveSinkFailureDuringDistributedSchemaEvolution() throws Exception {
        PipelineExecution execution = createPipelineExecution();
        CompletableFuture<Throwable> executionFailure =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                execution.execute();
                                throw new AssertionError("Pipeline execution should fail.");
                            } catch (Throwable t) {
                                return t;
                            }
                        });

        try {
            Assertions.assertThat(FailingDataSinkFactory.awaitMetadataApplying(EVENT_TIMEOUT))
                    .as("Metadata applier should block while the coordinator is EVOLVING.")
                    .isTrue();
            Assertions.assertThat(FailingDataSinkFactory.awaitSinkFailure(EVENT_TIMEOUT))
                    .as("A sink subtask should fail while schema evolution is in progress.")
                    .isTrue();

            Throwable failure =
                    executionFailure.get(EVENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assertions.assertThat(failure)
                    .as("The original sink failure should remain the root cause.")
                    .hasRootCauseMessage(FailingDataSinkFactory.ORIGINAL_FAILURE_MESSAGE);
            Assertions.assertThat(getRootCause(failure).getSuppressed())
                    .as(
                            "The schema evolution state error should only be attached as a suppressed exception.")
                    .anySatisfy(
                            suppressed ->
                                    Assertions.assertThat(suppressed)
                                            .isInstanceOf(IllegalStateException.class)
                                            .hasMessage("Unexpected evolving status: EVOLVING"));
        } finally {
            FailingDataSinkFactory.releaseMetadataApplier();
            executionFailure.cancel(true);
        }
    }

    private static PipelineExecution createPipelineExecution() {
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(DistributedSourceOptions.TABLE_COUNT, 1);
        sourceConfig.set(DistributedSourceOptions.DISTRIBUTED_TABLES, true);
        SourceDef sourceDef =
                new SourceDef(
                        DistributedDataSourceFactory.IDENTIFIER,
                        "Distributed Source",
                        sourceConfig);

        SinkDef sinkDef =
                new SinkDef(FailingDataSinkFactory.IDENTIFIER, "Failing Sink", new Configuration());

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);
        pipelineConfig.set(
                PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR, SchemaChangeBehavior.LENIENT);
        return FlinkPipelineComposer.ofMiniCluster()
                .compose(
                        new PipelineDef(
                                sourceDef,
                                sinkDef,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                pipelineConfig));
    }

    private static Throwable getRootCause(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }
}
