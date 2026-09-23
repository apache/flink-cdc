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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.testsource.recovery.RecoveryDataSink;
import org.apache.flink.cdc.composer.testsource.recovery.RecoveryDataSinkFactory;
import org.apache.flink.cdc.composer.testsource.recovery.RecoveryDataSourceFactory;
import org.apache.flink.cdc.composer.testsource.recovery.RecoverySourceFunction;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests regular and distributed schema coordinator recovery through a real MiniCluster. */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class FlinkSchemaCoordinatorRecoveryITCase {

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
                            .setNumberSlotsPerTaskManager(4)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    @AfterEach
    void cleanup() {
        ValuesDatabase.clear();
        RecoveryDataSink.reset();
        RecoverySourceFunction.reset();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void replaysFailedSchemaChangeAfterCheckpointAndContinues(boolean distributed)
            throws Exception {
        RecoveryDataSink.reset();
        RecoverySourceFunction.reset();
        ValuesDatabase.clear();

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(RecoveryDataSourceFactory.PARALLEL_METADATA_SOURCE, distributed);
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, distributed ? 2 : 1);
        pipelineConfig.set(PipelineOptions.PIPELINE_NAME, "schema-coordinator-recovery");

        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        composer.getEnv().enableCheckpointing(50L);
        composer.getEnv().getCheckpointConfig().setMinPauseBetweenCheckpoints(2_000L);
        org.apache.flink.configuration.Configuration restartConfig =
                new org.apache.flink.configuration.Configuration();
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        composer.getEnv().configure(restartConfig);
        composer.compose(
                new PipelineDef(
                        new SourceDef(
                                RecoveryDataSourceFactory.IDENTIFIER,
                                "Recovery test source",
                                sourceConfig),
                        new SinkDef(
                                RecoveryDataSinkFactory.IDENTIFIER,
                                "Recovery test sink",
                                new Configuration()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig));

        JobClient jobClient = composer.getEnv().executeAsync("schema-coordinator-recovery");
        CompletableFuture<JobExecutionResult> executionResult = jobClient.getJobExecutionResult();
        try {
            executionResult.get();

            TableId tableId = TableId.tableId("recovery", "schema", "orders");
            assertThat(RecoveryDataSink.getFailures()).isEqualTo(1);
            assertThat(RecoverySourceFunction.getRunAttempts()).isGreaterThanOrEqualTo(2);
            assertThat(RecoverySourceFunction.getFirstColumnEmissions()).isEqualTo(2);
            assertThat(RecoveryDataSink.getFirstColumnApplications()).isEqualTo(2);
            assertThat(ValuesDatabase.getTableSchema(tableId).getColumnNames())
                    .containsExactly("id", "value", "extra_v1", "extra_v2");
            assertThat(ValuesDatabase.getResults(tableId))
                    .containsExactly(
                            "recovery.schema.orders:id=1;value=after;extra_v1=v1;extra_v2=v2");
        } finally {
            cancelAndWait(jobClient, executionResult);
        }
    }

    private static void cancelAndWait(
            JobClient jobClient, CompletableFuture<JobExecutionResult> executionResult)
            throws Exception {
        if (!executionResult.isDone()) {
            jobClient.cancel().get(30, TimeUnit.SECONDS);
            try {
                executionResult.get(30, TimeUnit.SECONDS);
            } catch (CancellationException | ExecutionException ignored) {
                // A cancelled job completes the result future exceptionally.
            }
        }
    }
}
