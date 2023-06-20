/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.MySqlContainer;
import com.ververica.cdc.connectors.mysql.testutils.TestTable;
import com.ververica.cdc.connectors.mysql.testutils.TestTableSchemas;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import io.debezium.connector.mysql.MySqlConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for validating specifying starting offset. */
public class SpecificStartingOffsetITCase {
    private static final Logger LOG = LoggerFactory.getLogger(SpecificStartingOffsetITCase.class);
    @RegisterExtension static MiniClusterExtension miniCluster = new MiniClusterExtension();

    @SuppressWarnings("unchecked")
    private final MySqlContainer mysql =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride(
                                    buildMySqlConfigWithTimezone(
                                            getResourceFolder(), getSystemTimeZone()))
                            .withSetupSQL("docker/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(mysql, "customer", "mysqluser", "mysqlpw");
    private final TestTable customers =
            new TestTable(customDatabase, "customers", TestTableSchemas.CUSTOMERS);

    private MySqlConnection connection;

    @BeforeEach
    void prepare() throws Exception {
        mysql.start();
        connection = getConnection();
        customDatabase.createAndInitialize();
        flushLogs();
    }

    @AfterEach
    void tearDown() throws Exception {
        customDatabase.dropDatabase();
        connection.close();
        mysql.stop();
    }

    @Test
    void testStartingFromEarliestOffset() throws Exception {
        // Purge binary log at first
        purgeBinaryLogs();

        // Insert new data after the purge
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15213, 'Alice', 'Rome', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15513, 'Bob', 'Milan', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (18213, 'Charlie', 'Paris', '123456987');",
                        customers.getTableId()));

        // Build Flink job
        StreamExecutionEnvironment env = getExecutionEnvironment();
        MySqlSource<RowData> source =
                getSourceBuilder().startupOptions(StartupOptions.earliest()).build();
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "earliest-offset-test");
        CollectResultIterator<RowData> iterator = addCollector(env, stream);

        // Copy transformations into another env
        StreamExecutionEnvironment restoredEnv = getExecutionEnvironment();
        duplicateTransformations(env, restoredEnv);

        // Execute job and validate results
        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);
        List<String> rows = fetchRowData(iterator, 3, customers::stringify);
        assertThat(rows)
                .containsExactly(
                        "+I[15213, Alice, Rome, 123456987]",
                        "+I[15513, Bob, Milan, 123456987]",
                        "+I[18213, Charlie, Paris, 123456987]");

        // Take a savepoint
        Path savepointDir = Files.createTempDirectory("earliest-offset-test");
        String savepointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                savepointDir.toAbsolutePath().toString(),
                                SavepointFormatType.DEFAULT)
                        .get();
        jobClient.cancel();

        // Make some changes after the savepoint
        executeStatements(
                String.format(
                        "UPDATE %s SET name = 'Alicia' WHERE id = 15213", customers.getTableId()));

        // Restart the job from savepoint and check result
        setupSavepoint(restoredEnv, savepointPath);
        JobClient restoredJobClient = restoredEnv.executeAsync();
        iterator.setJobClient(restoredJobClient);
        List<String> rowsAfterRestored = fetchRowData(iterator, 2, customers::stringify);
        assertThat(rowsAfterRestored)
                .containsExactly(
                        "-U[15213, Alice, Rome, 123456987]", "+U[15213, Alicia, Rome, 123456987]");

        restoredJobClient.cancel();
    }

    @Test
    void testStartingFromSpecificOffset() throws Exception {
        // Purge binary log at first
        purgeBinaryLogs();

        // Record current binlog offset
        BinlogOffset startingOffset = DebeziumUtils.currentBinlogOffset(connection);

        // Insert new data after the purge
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15213, 'Alice', 'Rome', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15513, 'Bob', 'Milan', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (18213, 'Charlie', 'Paris', '123456987');",
                        customers.getTableId()));

        // Build Flink job
        StreamExecutionEnvironment env = getExecutionEnvironment();
        MySqlSource<RowData> source =
                getSourceBuilder()
                        .startupOptions(
                                StartupOptions.specificOffset(
                                        startingOffset.getFilename(), startingOffset.getPosition()))
                        .build();
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "specific-offset-test");
        CollectResultIterator<RowData> iterator = addCollector(env, stream);

        // Copy transformations into another env
        StreamExecutionEnvironment restoredEnv = getExecutionEnvironment();
        duplicateTransformations(env, restoredEnv);

        // Execute job and validate results
        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);
        List<String> rows = fetchRowData(iterator, 3, customers::stringify);
        assertThat(rows)
                .containsExactly(
                        "+I[15213, Alice, Rome, 123456987]",
                        "+I[15513, Bob, Milan, 123456987]",
                        "+I[18213, Charlie, Paris, 123456987]");

        // Take a savepoint
        Path savepointDir = Files.createTempDirectory("specific-offset-test");
        String savepointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                savepointDir.toAbsolutePath().toString(),
                                SavepointFormatType.DEFAULT)
                        .get();
        jobClient.cancel();

        // Make some changes after the savepoint
        executeStatements(
                String.format(
                        "UPDATE %s SET name = 'Alicia' WHERE id = 15213", customers.getTableId()));

        // Restart the job from savepoint and check result
        setupSavepoint(restoredEnv, savepointPath);
        JobClient restoredJobClient = restoredEnv.executeAsync("snapshotSplitTest");
        iterator.setJobClient(restoredJobClient);
        List<String> rowsAfterRestored = fetchRowData(iterator, 2, customers::stringify);
        assertThat(rowsAfterRestored)
                .containsExactly(
                        "-U[15213, Alice, Rome, 123456987]", "+U[15213, Alicia, Rome, 123456987]");

        restoredJobClient.cancel();
    }

    @Test
    void testStartingFromTimestampOffset() throws Exception {
        // Purge binary log at first
        purgeBinaryLogs();

        // Insert new data after the purge
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15213, 'Alice', 'Rome', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (15513, 'Bob', 'Milan', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (18213, 'Charlie', 'Paris', '123456987');",
                        customers.getTableId()));

        // Record current timestamp
        Thread.sleep(1000);
        StartupOptions startupOptions = StartupOptions.timestamp(System.currentTimeMillis());

        // After recording the timestamp, insert some new data
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (19613, 'Tom', 'NewYork', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (20913, 'Cat', 'Washington', '123456987');",
                        customers.getTableId()));
        executeStatements(
                String.format(
                        "INSERT INTO %s VALUES (23313, 'Mouse', 'Seattle', '123456987');",
                        customers.getTableId()));

        // Build Flink job
        StreamExecutionEnvironment env = getExecutionEnvironment();
        MySqlSource<RowData> source = getSourceBuilder().startupOptions(startupOptions).build();
        DataStreamSource<RowData> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "timestamp-offset-test");
        CollectResultIterator<RowData> iterator = addCollector(env, stream);

        // Copy transformations into another env
        StreamExecutionEnvironment restoredEnv = getExecutionEnvironment();
        duplicateTransformations(env, restoredEnv);

        // Execute job and validate results
        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);
        List<String> rows = fetchRowData(iterator, 3, customers::stringify);
        assertThat(rows)
                .containsExactly(
                        "+I[19613, Tom, NewYork, 123456987]",
                        "+I[20913, Cat, Washington, 123456987]",
                        "+I[23313, Mouse, Seattle, 123456987]");

        // Take a savepoint
        Path savepointDir = Files.createTempDirectory("timestamp-offset-test");
        String savepointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                savepointDir.toAbsolutePath().toString(),
                                SavepointFormatType.DEFAULT)
                        .get();
        jobClient.cancel();

        // Make some changes after the savepoint
        executeStatements(
                String.format(
                        "UPDATE %s SET name = 'George' WHERE id = 18213", customers.getTableId()));

        // Restart the job from savepoint and check result
        setupSavepoint(restoredEnv, savepointPath);
        JobClient restoredJobClient = restoredEnv.executeAsync("snapshotSplitTest");
        iterator.setJobClient(restoredJobClient);
        List<String> rowsAfterRestored = fetchRowData(iterator, 2, customers::stringify);
        assertThat(rowsAfterRestored)
                .containsExactly(
                        "-U[18213, Charlie, Paris, 123456987]",
                        "+U[18213, George, Paris, 123456987]");

        restoredJobClient.cancel();
    }

    private MySqlSourceBuilder<RowData> getSourceBuilder() {
        return MySqlSource.<RowData>builder()
                .hostname(mysql.getHost())
                .port(mysql.getDatabasePort())
                .username(customDatabase.getUsername())
                .password(customDatabase.getPassword())
                .databaseList(customDatabase.getDatabaseName())
                .tableList(customers.getTableId())
                .deserializer(customers.getDeserializer());
    }

    private MySqlConnection getConnection() {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.hostname", mysql.getHost());
        properties.put("database.port", String.valueOf(mysql.getDatabasePort()));
        properties.put("database.user", customDatabase.getUsername());
        properties.put("database.password", customDatabase.getPassword());
        io.debezium.config.Configuration configuration =
                io.debezium.config.Configuration.from(properties);
        return DebeziumUtils.createMySqlConnection(configuration, new Properties());
    }

    private void executeStatements(String... statements) throws Exception {
        connection.execute(statements);
        connection.commit();
    }

    private void flushLogs() throws Exception {
        executeStatements("FLUSH LOGS;");
    }

    private void purgeBinaryLogs() throws Exception {
        BinlogOffset currentOffset = DebeziumUtils.currentBinlogOffset(connection);
        String currentBinlogFilename = currentOffset.getFilename();
        executeStatements(String.format("PURGE BINARY LOGS TO '%s'", currentBinlogFilename));
    }

    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env, DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getTransformation().getOutputType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig());
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        return iterator;
    }

    private List<String> fetchRowData(
            Iterator<RowData> iter, int size, Function<RowData, String> stringifier) {
        List<RowData> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            RowData row = iter.next();
            rows.add(row);
            size--;
        }
        return rows.stream().map(stringifier).collect(Collectors.toList());
    }

    private static String buildMySqlConfigWithTimezone(File resourceDirectory, String timezone) {
        try {
            TemporaryFolder tempFolder = new TemporaryFolder(resourceDirectory);
            tempFolder.create();
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(Paths.get(folder.getPath(), "my.cnf"));
            String mysqldConf =
                    "[mysqld]\n"
                            + "binlog_format = row\n"
                            + "log_bin = mysql-bin\n"
                            + "server-id = 223344\n"
                            + "binlog_row_image = FULL\n"
                            + "gtid_mode = on\n"
                            + "enforce_gtid_consistency = on\n";
            String timezoneConf = "default-time_zone = '" + timezone + "'\n";
            Files.write(
                    cnf,
                    Collections.singleton(mysqldConf + timezoneConf),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceDirectory.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }

    private static File getResourceFolder() {
        try {
            return Paths.get(
                            Objects.requireNonNull(
                                            SpecificStartingOffsetITCase.class
                                                    .getClassLoader()
                                                    .getResource("."))
                                    .toURI())
                    .toFile();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Get Resource File Directory fail");
        }
    }

    private static String getSystemTimeZone() {
        return ZoneId.systemDefault().toString();
    }

    private void setupSavepoint(StreamExecutionEnvironment env, String savepointPath)
            throws Exception {
        // restore from savepoint
        // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> clazz =
                classLoader.loadClass(
                        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
        Field field = clazz.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = (Configuration) field.get(env);
        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, savepointPath);
    }

    private void duplicateTransformations(
            StreamExecutionEnvironment source, StreamExecutionEnvironment target) {
        source.getTransformations().forEach(target::addOperator);
    }

    private StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        return env;
    }
}
