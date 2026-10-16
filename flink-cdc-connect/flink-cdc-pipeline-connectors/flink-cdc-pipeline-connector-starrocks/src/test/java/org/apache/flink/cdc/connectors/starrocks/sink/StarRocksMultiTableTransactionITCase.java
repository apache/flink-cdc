/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.starrocks.sink.utils.StarRocksContainer;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.JDBC_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.LOAD_URL;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_MULTI_TABLE_TRANSACTION_BUFFER_SIZE;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_MULTI_TABLE_TRANSACTION_ENABLED;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT tests for {@link StarRocksDataSink} with multi-table transaction stream load enabled. Requires
 * a StarRocks server >= 4.0, hence a dedicated 4.x container.
 */
class StarRocksMultiTableTransactionITCase {

    private static final String STARROCKS_4X_IMAGE = "4.0.16";

    private static final int DEFAULT_PARALLELISM = 1;

    private static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    protected static final StarRocksContainer STARROCKS_CONTAINER =
            new StarRocksContainer(STARROCKS_4X_IMAGE);

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .withHaLeadershipControl()
                                    .build()));

    @BeforeAll
    public static void startContainers() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        Startables.deepStart(Stream.of(STARROCKS_CONTAINER)).join();

        long startWaitingTimestamp = System.currentTimeMillis();
        new LogMessageWaitStrategy()
                .withRegEx(".*Enjoy the journey to StarRocks blazing-fast lake-house engine!.*\\s")
                .withTimes(1)
                .withStartupTimeout(
                        Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .waitUntilReady(STARROCKS_CONTAINER);

        while (!checkBackendAvailability()) {
            if (System.currentTimeMillis() - startWaitingTimestamp
                    > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                throw new RuntimeException("StarRocks backend startup timed out.");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                // ignore and check next round
            }
        }
    }

    @AfterAll
    public static void stopContainers() {
        STARROCKS_CONTAINER.stop();
    }

    @BeforeEach
    public void initializeDatabaseAndTables() {
        executeSql(
                String.format(
                        "CREATE DATABASE IF NOT EXISTS `%s`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));
        executeSql(
                String.format(
                        "CREATE TABLE `%s`.`products` (id INT NOT NULL, name VARCHAR(51), "
                                + "updated_at DATETIME) PRIMARY KEY (`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "
                                + "PROPERTIES (\"replication_num\" = \"1\");",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));
        executeSql(
                String.format(
                        "CREATE TABLE `%s`.`orders` (id INT NOT NULL, product_id INT, quantity INT) "
                                + "PRIMARY KEY (`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 "
                                + "PROPERTIES (\"replication_num\" = \"1\");",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));
    }

    @AfterEach
    public void destroyDatabaseAndTables() {
        executeSql(
                String.format(
                        "DROP TABLE IF EXISTS `%s`.`orders`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));
        executeSql(
                String.format(
                        "DROP TABLE IF EXISTS `%s`.`products`;",
                        StarRocksContainer.STARROCKS_DATABASE_NAME));
        executeSql(
                String.format(
                        "DROP DATABASE IF EXISTS %s;", StarRocksContainer.STARROCKS_DATABASE_NAME));
    }

    @Test
    void testMultiTableWriteWithSharedTransaction() throws Exception {
        TableId products = TableId.tableId(StarRocksContainer.STARROCKS_DATABASE_NAME, "products");
        TableId orders = TableId.tableId(StarRocksContainer.STARROCKS_DATABASE_NAME, "orders");

        DataStream<Event> stream =
                env.fromCollection(
                        generateEvents(products, orders), TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(LOAD_URL, STARROCKS_CONTAINER.getLoadUrl())
                        .set(JDBC_URL, STARROCKS_CONTAINER.getJdbcUrl())
                        .set(USERNAME, StarRocksContainer.STARROCKS_USERNAME)
                        .set(PASSWORD, StarRocksContainer.STARROCKS_PASSWORD)
                        .set(SINK_MULTI_TABLE_TRANSACTION_ENABLED, true)
                        .set(SINK_MULTI_TABLE_TRANSACTION_BUFFER_SIZE, 1024 * 1024L);

        Sink<Event> starRocksSink =
                ((FlinkSinkProvider) createStarRocksDataSink(config).getEventSinkProvider())
                        .getSink();
        stream.sinkTo(starRocksSink);

        env.execute("Multi-table transaction to StarRocks Sink");

        List<String> productsRows = fetchTableContent("products", 3);
        assertThat(productsRows)
                .containsExactlyInAnyOrder(
                        "1 | keyboard | 2023-01-01 00:00:00.0",
                        "2 | monitor | 2023-01-01 00:00:00.0");

        List<String> ordersRows = fetchTableContent("orders", 3);
        assertThat(ordersRows).containsExactlyInAnyOrder("100 | 1 | 2", "101 | 2 | 1");
    }

    private static DataSink createStarRocksDataSink(Configuration factoryConfiguration) {
        StarRocksDataSinkFactory factory = new StarRocksDataSinkFactory();
        Configuration pipelineConfiguration =
                Configuration.fromMap(Collections.singletonMap("local-time-zone", "UTC"));
        return factory.createDataSink(
                new FactoryContext(factoryConfiguration, pipelineConfiguration));
    }

    private List<Event> generateEvents(TableId products, TableId orders) {
        Schema productsSchema =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("name", DataTypes.VARCHAR(17)))
                        .column(Column.physicalColumn("updated_at", DataTypes.TIMESTAMP_LTZ(6)))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator productsGenerator =
                new BinaryRecordDataGenerator(
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.VARCHAR(17),
                                DataTypes.TIMESTAMP_LTZ(6)));

        Schema ordersSchema =
                Schema.newBuilder()
                        .column(Column.physicalColumn("id", DataTypes.INT()))
                        .column(Column.physicalColumn("product_id", DataTypes.INT()))
                        .column(Column.physicalColumn("quantity", DataTypes.INT()))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator ordersGenerator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.INT()));

        List<Event> events = new ArrayList<>();
        events.add(new CreateTableEvent(products, productsSchema));
        events.add(
                DataChangeEvent.insertEvent(
                        products,
                        productsGenerator.generate(
                                new Object[] {
                                    1,
                                    BinaryStringData.fromString("keyboard"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })));
        events.add(
                DataChangeEvent.insertEvent(
                        products,
                        productsGenerator.generate(
                                new Object[] {
                                    2,
                                    BinaryStringData.fromString("monitor"),
                                    LocalZonedTimestampData.fromInstant(
                                            Instant.parse("2023-01-01T00:00:00.000Z"))
                                })));
        events.add(new CreateTableEvent(orders, ordersSchema));
        events.add(
                DataChangeEvent.insertEvent(
                        orders, ordersGenerator.generate(new Object[] {100, 1, 2})));
        events.add(
                DataChangeEvent.insertEvent(
                        orders, ordersGenerator.generate(new Object[] {101, 2, 1})));
        return events;
    }

    private static void executeSql(String sql) {
        try {
            Container.ExecResult rs =
                    STARROCKS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e " + sql);
            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to execute SQL." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL.", e);
        }
    }

    private static boolean checkBackendAvailability() {
        try {
            Container.ExecResult rs =
                    STARROCKS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e SHOW BACKENDS\\G");
            if (rs.getExitCode() != 0) {
                return false;
            }
            return rs.getStdout()
                    .contains("*************************** 1. row ***************************");
        } catch (Exception e) {
            return false;
        }
    }

    private List<String> fetchTableContent(String tableName, int columnCount) throws SQLException {
        List<String> results = new ArrayList<>();
        ResultSet rs =
                STARROCKS_CONTAINER
                        .createConnection("")
                        .createStatement()
                        .executeQuery(
                                String.format(
                                        "SELECT * FROM %s.%s",
                                        StarRocksContainer.STARROCKS_DATABASE_NAME, tableName));
        while (rs.next()) {
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columns.add(rs.getString(i));
            }
            results.add(String.join(" | ", columns));
        }
        return results;
    }

    /** Minimal factory context used to create the data sink. */
    private static class FactoryContext implements Factory.Context {

        private final Configuration factoryConfiguration;
        private final Configuration pipelineConfiguration;

        private FactoryContext(
                Configuration factoryConfiguration, Configuration pipelineConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
            this.pipelineConfiguration = pipelineConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return pipelineConfiguration;
        }

        @Override
        public ClassLoader getClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }
    }
}
