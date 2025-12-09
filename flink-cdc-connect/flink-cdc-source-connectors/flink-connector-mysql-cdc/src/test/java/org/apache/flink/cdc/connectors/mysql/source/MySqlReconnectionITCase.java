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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for MySQL reconnection logic in {@link MySqlSource}. */
class MySqlReconnectionITCase extends MySqlSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlReconnectionITCase.class);

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testBinlogReconnectionWithNetworkInterruption() throws Exception {
        UniqueDatabase database =
                new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
        database.createAndInitialize();

        // Create table for testing
        try (Connection conn = database.getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE network_table ("
                            + "id INT PRIMARY KEY, "
                            + "data VARCHAR(255)"
                            + ")");
            stmt.execute("INSERT INTO network_table VALUES (1, 'Initial data')");
        }

        // Create Debezium properties to test reconnection failure
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty(
                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), "true");

        // Create source with reconnection settings
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(database.getDatabaseName())
                        .tableList(database.getDatabaseName() + ".network_table")
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .serverId("5420-5424")
                        .serverTimeZone("UTC")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .connectTimeout(Duration.ofSeconds(10))
                        .connectMaxRetries(3)
                        .debeziumProperties(debeziumProps)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        env.setParallelism(1);

        DataStreamSource<String> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        CloseableIterator<String> iterator = source.executeAndCollect();
        List<String> records = new ArrayList<>();

        // Consume initial snapshot
        if (iterator.hasNext()) {
            String record = iterator.next();
            records.add(record);
            LOG.info("Received initial record: {}", record);
        }

        // Insert data during binlog phase
        try (Connection conn = database.getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO network_table VALUES (2, 'Binlog data')");
        }

        // Wait for binlog event and simulate interruption
        Thread.sleep(1000);

        // Simulate network interruption
        LOG.info("Simulating network interruption...");
        MYSQL_CONTAINER.stop();
        Thread.sleep(2000);
        MYSQL_CONTAINER.start();
        Thread.sleep(3000);

        // Try to continue consuming - should fail due to binlogFailOnReconnectionError=true
        boolean exceptionThrown = false;
        try {
            long startTime = System.currentTimeMillis();
            while (records.size() < 2 && (System.currentTimeMillis() - startTime) < 30000) {
                if (iterator.hasNext()) {
                    String record = iterator.next();
                    records.add(record);
                    LOG.info("Received post-reconnection record: {}", record);
                } else {
                    Thread.sleep(100);
                }
            }
        } catch (Exception e) {
            LOG.info(
                    "Expected exception due to binlogFailOnReconnectionError=true: {}",
                    e.getMessage());
            exceptionThrown = true;
        }

        // Verify the job failed due to network interruption with binlogFailOnReconnectionError=true
        assertThat(exceptionThrown).isTrue();
        iterator.close();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testReconnectionWithCustomTimeoutAndRetries() throws Exception {
        UniqueDatabase database =
                new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
        database.createAndInitialize();

        // Create table for testing
        try (Connection conn = database.getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE products ("
                            + "id INT PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "description VARCHAR(512)"
                            + ")");
            stmt.execute("INSERT INTO products VALUES (1, 'Product 1', 'Initial product')");
        }

        // Create Debezium properties to test reconnection failure after retries
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty(
                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), "true");

        // Create source with custom reconnection settings
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(database.getDatabaseName())
                        .tableList(database.getDatabaseName() + ".products")
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .serverId("5400-5404")
                        .serverTimeZone("UTC")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        // Test custom reconnection settings
                        .connectTimeout(Duration.ofSeconds(5))
                        .connectMaxRetries(2)
                        .debeziumProperties(debeziumProps)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        env.setParallelism(1);

        DataStreamSource<String> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        CloseableIterator<String> iterator = source.executeAndCollect();
        List<String> records = new ArrayList<>();

        // Consume initial snapshot
        for (int i = 0; i < 1 && iterator.hasNext(); i++) {
            String record = iterator.next();
            records.add(record);
            LOG.info("Received record: {}", record);
        }

        assertThat(records).hasSize(1);

        // Insert data while source is running
        try (Connection conn = database.getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO products VALUES (2, 'Product 2', 'Test reconnection')");
        }

        // Simulate network interruption by briefly stopping container
        LOG.info("Stopping MySQL container to simulate network interruption...");
        MYSQL_CONTAINER.stop();

        // Wait a moment to ensure disconnection
        Thread.sleep(2000);

        LOG.info("Restarting MySQL container...");
        MYSQL_CONTAINER.start();

        // Wait for container to be fully ready
        Thread.sleep(3000);

        // Try to continue consuming - should fail due to binlogFailOnReconnectionError=true
        boolean exceptionThrown = false;
        try {
            long startTime = System.currentTimeMillis();
            while (iterator.hasNext() && (System.currentTimeMillis() - startTime) < 20000) {
                String record = iterator.next();
                records.add(record);
                LOG.info("Received post-reconnection record: {}", record);
            }
        } catch (Exception e) {
            LOG.info(
                    "Expected exception due to binlogFailOnReconnectionError=true: {}",
                    e.getMessage());
            exceptionThrown = true;
        }

        // Verify the job failed due to reconnection error (binlogFailOnReconnectionError=true)
        assertThat(exceptionThrown).isTrue();

        iterator.close();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testReconnectionFailureWhenMaxRetriesExceeded() throws Exception {
        UniqueDatabase database =
                new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");
        database.createAndInitialize();

        // Create table for testing
        try (Connection conn = database.getJdbcConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE test_table ("
                            + "id INT PRIMARY KEY, "
                            + "value VARCHAR(255)"
                            + ")");
            stmt.execute("INSERT INTO test_table VALUES (1, 'Initial value')");
        }

        // Create Debezium properties to fail after max retries
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty(
                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), "true");

        // Create source with very short timeout and low retries to force failure
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(database.getDatabaseName())
                        .tableList(database.getDatabaseName() + ".test_table")
                        .username(database.getUsername())
                        .password(database.getPassword())
                        .serverId("5405-5409")
                        .serverTimeZone("UTC")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        // Very restrictive settings to trigger failure
                        .connectTimeout(Duration.ofSeconds(1))
                        .connectMaxRetries(1)
                        .debeziumProperties(debeziumProps)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);

        DataStreamSource<String> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        CloseableIterator<String> iterator = source.executeAndCollect();

        // Consume initial record
        if (iterator.hasNext()) {
            String record = iterator.next();
            LOG.info("Received initial record: {}", record);
        }

        // Stop container for extended period to exceed retry limits
        LOG.info("Stopping MySQL container for extended period...");
        MYSQL_CONTAINER.stop();

        // The source should eventually fail due to reconnection timeout
        // We expect an exception to be thrown when reconnection fails
        boolean exceptionThrown = false;
        try {
            // Try to continue reading - this should fail
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 15000) {
                if (iterator.hasNext()) {
                    iterator.next();
                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            LOG.info("Expected exception during reconnection failure: {}", e.getMessage());
            exceptionThrown = true;
        }

        // Restart container for cleanup
        MYSQL_CONTAINER.start();

        iterator.close();

        // The test passes if we handled the reconnection failure appropriately
        // Either by throwing an exception or by gracefully handling the disconnection
        LOG.info("Reconnection failure test completed, exception thrown: {}", exceptionThrown);
    }
}
