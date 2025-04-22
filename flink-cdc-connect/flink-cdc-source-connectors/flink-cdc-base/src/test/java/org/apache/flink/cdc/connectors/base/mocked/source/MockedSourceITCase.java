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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.cdc.common.utils.TestCaseUtils.repeatedCheckAndValidate;

/** Integrated test cases with {@link MockedIncrementalSource}. */
public class MockedSourceITCase {

    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static final int TABLE_COUNT = 3;
    private static final int RECORD_COUNT = 5;

    @BeforeEach
    void before() {
        System.setOut(new PrintStream(outCaptor));
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @AfterEach
    void after() {
        System.setOut(standardOut);
        outCaptor.reset();
    }

    @ParameterizedTest(name = "multipleStreamSplits: {0}")
    @ValueSource(booleans = {true, false})
    void testMockedSource(boolean multipleStreamSplits) throws Exception {
        MockedIncrementalSource source =
                new MockedIncrementalSource(
                        (subTask) ->
                                new MockedConfigFactory()
                                        .setTableCount(TABLE_COUNT)
                                        .setRecordCount(RECORD_COUNT)
                                        .setSplitSize(2)
                                        .setSplitMetaGroupSize(10)
                                        .setStartupOptions(StartupOptions.initial())
                                        .setRefreshInterval(Duration.ofMillis(100))
                                        .setMultipleStreamSplitsEnabled(multipleStreamSplits)
                                        .create(subTask),
                        new JsonDebeziumDeserializationSchema());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source")
                .setParallelism(4)
                .map(s -> s.replaceAll(",\"timestamp\":-?\\d+", ""))
                .print()
                .setParallelism(1);

        JobClient client = env.executeAsync("Mocked Source Test");
        waitUntilJobRunning(client);

        source.getDialect().getMockedDatabase().start(TABLE_COUNT * (RECORD_COUNT - 1));

        try {
            List<String> expected = new ArrayList<>();
            IntStream.rangeClosed(1, TABLE_COUNT)
                    .forEach(
                            i ->
                                    IntStream.rangeClosed(1, RECORD_COUNT)
                                            .forEach(
                                                    j ->
                                                            Stream.of(
                                                                            "{\"op\":\"+I\",\"id\":%d,\"name\":\"INITIAL_mock_ns.mock_scm.table_%d\"}",
                                                                            "{\"op\":\"+I\",\"id\":-%d,\"name\":\"INSERT_mock_ns.mock_scm.table_%d\"}",
                                                                            "{\"op\":\"-U\",\"id\":-%d,\"name\":\"INSERT_mock_ns.mock_scm.table_%d\"}",
                                                                            "{\"op\":\"+U\",\"id\":-%d,\"name\":\"UPDATE_mock_ns.mock_scm.table_%d\"}",
                                                                            "{\"op\":\"-D\",\"id\":-%d,\"name\":\"UPDATE_mock_ns.mock_scm.table_%d\"}")
                                                                    .map(
                                                                            fmtStr ->
                                                                                    String.format(
                                                                                            fmtStr,
                                                                                            j, i))
                                                                    .forEach(expected::add)));
            validateStdOut(expected.toArray(new String[0]));
        } catch (Exception e) {
            System.err.println("Failed to validate output. Stdout: ");
            System.err.println(outCaptor);
            throw e;
        } finally {
            source.getDialect().getMockedDatabase().stop();
            client.cancel().get();
        }
    }

    @Test
    void testStreamSplitsMoreThanParallelism() {
        MockedIncrementalSource source =
                new MockedIncrementalSource(
                        (subTask) ->
                                new MockedConfigFactory()
                                        .setTableCount(TABLE_COUNT)
                                        .setRecordCount(RECORD_COUNT)
                                        .setSplitSize(2)
                                        .setSplitMetaGroupSize(10)
                                        .setStartupOptions(StartupOptions.initial())
                                        .setRefreshInterval(Duration.ofMillis(100))
                                        .setMultipleStreamSplitsEnabled(true)
                                        .create(subTask),
                        new JsonDebeziumDeserializationSchema());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source")
                .setParallelism(2)
                .map(s -> s.replaceAll(",\"timestamp\":-?\\d+", ""))
                .print()
                .setParallelism(1);

        Assertions.assertThatThrownBy(env::execute)
                .rootCause()
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "4 stream splits generated, which is greater than current parallelism 2. Some splits might never be assigned.");
    }

    private void validateStdOut(String... lines) {
        repeatedCheckAndValidate(
                () -> {
                    Assertions.assertThat(outCaptor.toString().split("\n"))
                            .containsExactlyInAnyOrder(lines);
                    return true;
                },
                t -> t,
                Duration.ofMinutes(1),
                Duration.ofSeconds(1),
                singletonList(AssertionError.class));
    }

    private void waitUntilJobRunning(JobClient jobClient)
            throws InterruptedException, ExecutionException {
        do {
            Thread.sleep(5000L);
        } while (jobClient.getJobStatus().get() != RUNNING);
    }
}
