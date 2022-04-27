/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.base.source.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import com.ververica.cdc.connectors.oracle.utils.OracleTestUtils;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** Example Tests for {@link JdbcIncrementalSource}. */
public class OracleChangeEventSourceExampleTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(OracleChangeEventSourceExampleTest.class);

    private static final int DEFAULT_PARALLELISM = 2;
    private static final long DEFAULT_CHECKPOINT_INTERVAL = 1000;
    private static final OracleContainer oracleContainer =
            OracleTestUtils.ORACLE_CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG));

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .setConfiguration(new Configuration())
                            .withHaLeadershipControl()
                            .build());

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(oracleContainer)).join();
        LOG.info("Containers are started.");
    }

    @After
    public void teardown() {
        oracleContainer.stop();
    }

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        LOG.info(
                "getOraclePort:{},getUsername:{},getPassword:{}",
                oracleContainer.getOraclePort(),
                oracleContainer.getUsername(),
                oracleContainer.getPassword());

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");
        debeziumProperties.setProperty("log.mining.continuous.mine", "true");

        JdbcIncrementalSource<String> oracleChangeEventSource =
                new OracleSourceBuilder()
                        .hostname(oracleContainer.getHost())
                        .port(oracleContainer.getOraclePort())
                        .databaseList("XE")
                        .schemaList("DEBEZIUM")
                        .tableList("DEBEZIUM.PRODUCTS")
                        .username(oracleContainer.getUsername())
                        .password(oracleContainer.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .debeziumProperties(debeziumProperties)
                        .splitSize(3)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
        // set the source parallelism to 4
        env.fromSource(
                        oracleChangeEventSource,
                        WatermarkStrategy.noWatermarks(),
                        "OracleParallelSource")
                .setParallelism(DEFAULT_PARALLELISM)
                .print()
                .setParallelism(1);

        env.execute("Print Oracle Snapshot + RedoLog");
    }

    @Test
    public void testCheckpointAndRestoreTMFail() throws Exception {
        testCheckpointAndRestoreBase(FailoverType.TM);
    }

    @Test
    public void testCheckpointAndRestoreNone() throws Exception {
        testCheckpointAndRestoreBase(FailoverType.NONE);
    }

    public void testCheckpointAndRestoreBase(FailoverType failoverType) throws Exception {
        LOG.info(
                "getOraclePort:{},getUsername:{},getPassword:{}",
                oracleContainer.getOraclePort(),
                oracleContainer.getUsername(),
                oracleContainer.getPassword());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL)
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        final JdbcIncrementalSource<String> source = createOracleLogminerSource();
        DataStreamSource<String> dataStreamSource =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "OracleParallelSource")
                        .setParallelism(1);
        final DataStream<String> stream = dataStreamSource.returns(String.class);

        final DataStream<String> streamFailingInTheMiddleOfReading =
                RecordCounterToFail.wrapWithFailureAfter(stream, 2);

        final ClientAndIterator<String> client =
                DataStreamUtils.collectWithClient(
                        streamFailingInTheMiddleOfReading,
                        JdbcIncrementalSource.class.getSimpleName() + '-' + failoverType.name());
        final JobID jobId = client.client.getJobID();

        RecordCounterToFail.waitToFail();
        triggerFailover(
                failoverType,
                jobId,
                RecordCounterToFail::continueProcessing,
                miniClusterResource.getMiniCluster());

        final List<String> result = new ArrayList<>();
        while (result.size() < 9 && client.iterator.hasNext()) {
            result.add(client.iterator.next());
        }
        result.forEach(sourceRecord -> LOG.info("triggerFailover after record: {}", sourceRecord));
        LOG.info("result.size:{}", result.size());
        miniClusterResource.getMiniCluster().close();
        Assert.assertEquals(result.size(), 9);
    }

    // ------------------------------------------------------------------------------------------
    // Public Utilities
    // ------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private JdbcIncrementalSource<String> createOracleLogminerSource() {
        return basicSourceBuilder(oracleContainer).build();
    }

    private OracleSourceBuilder<String> basicSourceBuilder(OracleContainer oracleContainer) {
        Properties debeziumProperties = new Properties();
        // init debezium properties
        debeziumProperties.setProperty("log.mining.strategy", "online_catalog");
        debeziumProperties.setProperty("log.mining.continuous.mine", "true");

        OracleSourceBuilder<String> oracleSourceBuilder =
                new OracleSourceBuilder()
                        .hostname(oracleContainer.getHost())
                        .port(oracleContainer.getOraclePort())
                        .databaseList("XE")
                        .schemaList("DEBEZIUM")
                        .tableList("DEBEZIUM.PRODUCTS")
                        .username(oracleContainer.getUsername())
                        .password(oracleContainer.getPassword())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .debeziumProperties(debeziumProperties)
                        .splitSize(3);
        return oracleSourceBuilder;
    }

    /**
     * A simple implementation of {@link DebeziumDeserializationSchema} which just forward the
     * {@link SourceRecord}.
     */
    public static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 2975058057832211229L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }

    private enum FailoverType {
        NONE,
        TM,
        JM
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TM:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JM:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    // ------------------------------------------------------------------------
    //  mini cluster failover utilities
    // ------------------------------------------------------------------------

    private static class RecordCounterToFail {

        private static AtomicInteger records;
        private static CompletableFuture<Void> fail;
        private static CompletableFuture<Void> continueProcessing;

        private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int failAfter) {

            records = new AtomicInteger();
            fail = new CompletableFuture<>();
            continueProcessing = new CompletableFuture<>();
            return stream.map(
                    record -> {
                        LOG.info("normal get record: {}", record);
                        final boolean halfOfInputIsRead = records.incrementAndGet() > failAfter;
                        final boolean notFailedYet = !fail.isDone();
                        if (notFailedYet && halfOfInputIsRead) {
                            fail.complete(null);
                            continueProcessing.get();
                        }
                        return record;
                    });
        }

        private static void waitToFail() throws ExecutionException, InterruptedException {
            Thread.sleep((long) (DEFAULT_CHECKPOINT_INTERVAL * 1.5));
            fail.get();
        }

        private static void continueProcessing() {
            continueProcessing.complete(null);
        }
    }
}
