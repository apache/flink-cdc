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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MariaDbContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTable;
import org.apache.flink.cdc.connectors.mysql.testutils.TestTableSchemas;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectResultIteratorAdapter;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration Test for MariaDB restores from savepoint. The snapshot is taken while the source is
 * in the binlog phase, a change is made after the savepoint, and the job is restored from the
 * savepoint. Asserting that the post-savepoint UPDATE is captured exactly once proves the MariaDB
 * GTID offset survives a savepoint round trip: the stored {@code @@gtid_binlog_pos} text is
 * recovered as a MariaDB offset (stateless {@link
 * org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset} + {@code
 * GtidStrategies.detect}), and {@code MariaDbGtidStrategy.fixRestoredGtidSet} drives the resume -
 * the restore path that was previously only unit-test. The equivalent MySQL sp/retore path is
 * covered by {@link SpecificStartingOffsetITCase}
 */
class MariaDbDialectSavepointITCase {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDbDialectSavepointITCase.class);

    @RegisterExtension static MiniClusterExtension miniCluster = new MiniClusterExtension();

    private MariaDbContainer mariaDbContainer;
    private final TestTable customers =
            new TestTable("test", "customers", TestTableSchemas.CUSTOMERS);

    @BeforeEach
    void setup() throws Exception {
        mariaDbContainer = new MariaDbContainer();
        mariaDbContainer.withLogConsumer(new Slf4jLogConsumer(LOG));
        LOG.info("Starting MariaDbDialectSavepointITCase...");
        Startables.deepStart(Stream.of(mariaDbContainer)).join();
        LOG.info("Started MariaDbDialectSavepointITCase.");
        initializeCustomersTable();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mariaDbContainer != null) {
            mariaDbContainer.stop();
        }
    }

    @Test
    void testSavepointAndRestoreAcrossBinlog() throws Exception {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        MySqlSource<RowData> source =
                getSourceBuilder().startupOptions(StartupOptions.initial()).build();
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "mariadb-savepoint-test");
        CollectResultIterator<RowData> iterator = addCollector(env, stream);

        StreamExecutionEnvironment restoreEnv = getExecutionEnvironment();
        // duplicateTransformations
        env.getTransformations().forEach(restoreEnv::addOperator);

        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);
        List<String> snapshotRows = fetchRowData(iterator, 3, customers::stringify);
        assertThat(snapshotRows)
                .containsExactlyInAnyOrder(
                        "+I[15213, Alice, Rome, 123456987]",
                        "+I[15513, Bob, Milan, 123456987]",
                        "+I[18213, Adrian, Shenzhen, 123456987]");

        // Take a sp while the source is in the binlog phase. The sp stores a
        // MariaDB "domain-server-sequence" GTID offset (read from @@gtid_binlog_pos)
        Path spDir = Files.createTempDirectory("mariadb-savepoint-test");
        String spPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                spDir.toAbsolutePath().toString(),
                                SavepointFormatType.DEFAULT)
                        .get();
        jobClient.cancel();

        executeStatement("UPDATE customers SET name = 'Alicia' WHERE id = 15213");

        // Restore from the savepoint and check the post-savepoint change is captured exactly-once
        setupSavepoint(restoreEnv, spPath);
        JobClient restoredJobClient = restoreEnv.executeAsync();
        iterator.setJobClient(restoredJobClient);
        List<String> rowsAfterRestore = fetchRowData(iterator, 2, customers::stringify);
        assertThat(rowsAfterRestore)
                .containsExactly(
                        "-U[15213, Alice, Rome, 123456987]", "+U[15213, Alicia, Rome, 123456987]");

        restoredJobClient.cancel();
    }

    private MySqlSourceBuilder<RowData> getSourceBuilder() {
        return MySqlSource.<RowData>builder()
                .hostname(mariaDbContainer.getHost())
                .port(mariaDbContainer.getDatabasePort())
                .username(mariaDbContainer.getUsername())
                .password(mariaDbContainer.getPassword())
                .databaseList(mariaDbContainer.getDatabaseName())
                .tableList(customers.getTableId())
                .dialect("mariadb")
                .serverTimeZone("UTC")
                .serverId(getServerId())
                .deserializer(customers.getDeserializer());
    }

    private void initializeCustomersTable() throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "CREATE  TABLE customers ("
                            + "id BIGINT NOT NULL PRIMARY KEY, "
                            + "name VARCHAR(255),"
                            + "address VARCHAR(255),"
                            + "phone_number VARCHAR(255));");
            statement.execute(
                    "INSERT INTO customers VALUES "
                            + "(15213, 'Alice', 'Rome', '123456987'),"
                            + "(15513, 'Bob', 'Milan', '123456987'),"
                            + "(18213, 'Adrian', 'Shenzhen', '123456987');");
        }
    }

    private void executeStatement(String... statements) throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private Connection getJdbcConnection() throws Exception {
        return DriverManager.getConnection(
                mariaDbContainer.getJdbcUrl(),
                mariaDbContainer.getUsername(),
                mariaDbContainer.getPassword());
    }

    private String getServerId() {
        Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + 2);
    }

    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env, DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getTransformation()
                        .getOutputType()
                        .createSerializer(env.getConfig().getSerializerConfig());
        String accumulatorName = "dataStreamCollector_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        // Set both name and uid to the same value. The uid is used by Flink to generate
        // OperatorId via StreamGraphHasherV2.generateUserSpecifiedHash(uid), and the same
        // uid string must be passed to CollectResultIteratorAdapter for coordinator lookup
        String operatorUid = "Data stream collect sink";
        sink.name(operatorUid).uid(operatorUid);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        env.addOperator(sink.getTransformation());
        return new CollectResultIteratorAdapter<>(
                operatorUid,
                operator,
                serializer,
                accumulatorName,
                env.getCheckpointConfig(),
                10000L);
    }

    private List<String> fetchRowData(
            Iterator<RowData> iter, int size, Function<RowData, String> stringFunction) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            rows.add(iter.next());
            size--;
        }
        return rows.stream().map(stringFunction).collect(Collectors.toList());
    }

    private void setupSavepoint(StreamExecutionEnvironment env, String savepointPath)
            throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> clazz =
                classLoader.loadClass(
                        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
        Field field = clazz.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = (Configuration) field.get(env);
        configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
    }

    private StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(1)
                        .enableCheckpointing(100);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        return env;
    }
}
