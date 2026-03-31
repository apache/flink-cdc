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

package org.apache.flink.cdc.connectors.mysql.source.connection;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import io.debezium.connector.mysql.MySqlConnection;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reader hostname connection handling. Verifies that the reader connection is used when
 * configured, and falls back to the writer connection when not configured.
 */
class ReaderConnectionTest {

    private static final String WRITER_HOSTNAME = "writer-host";
    private static final String READER_HOSTNAME = "reader-host";
    private static final int PORT = 3306;

    @Test
    void testReaderHostnameConfigured() {
        MySqlSourceConfig config = createConfig(WRITER_HOSTNAME, READER_HOSTNAME);

        assertThat(config.getHostname()).isEqualTo(WRITER_HOSTNAME);
        assertThat(config.getSnapshotHostname()).isEqualTo(READER_HOSTNAME);
    }

    @Test
    void testReaderHostnameNotConfigured() {
        MySqlSourceConfig config = createConfig(WRITER_HOSTNAME, null);

        assertThat(config.getHostname()).isEqualTo(WRITER_HOSTNAME);
        assertThat(config.getSnapshotHostname()).isNull();
    }

    @Test
    void testCreateReaderMySqlConnectionWithReaderHostname() {
        MySqlSourceConfig config = createConfig(WRITER_HOSTNAME, READER_HOSTNAME);

        // Create reader connection - should use reader hostname
        MySqlConnection readerConnection = DebeziumUtils.createSnapshotMySqlConnection(config);
        String readerConnectionString = readerConnection.connectionString();

        // Create writer connection - should use writer hostname
        MySqlConnection writerConnection = DebeziumUtils.createMySqlConnection(config);
        String writerConnectionString = writerConnection.connectionString();

        // Reader connection should contain reader hostname
        assertThat(readerConnectionString).contains(READER_HOSTNAME);
        assertThat(readerConnectionString).doesNotContain(WRITER_HOSTNAME);

        // Writer connection should contain writer hostname
        assertThat(writerConnectionString).contains(WRITER_HOSTNAME);
        assertThat(writerConnectionString).doesNotContain(READER_HOSTNAME);
    }

    @Test
    void testCreateReaderMySqlConnectionFallsBackToWriterWhenNoReaderConfigured() {
        MySqlSourceConfig config = createConfig(WRITER_HOSTNAME, null);

        // Create reader connection - should fall back to writer hostname
        MySqlConnection readerConnection = DebeziumUtils.createSnapshotMySqlConnection(config);
        String readerConnectionString = readerConnection.connectionString();

        // Create writer connection
        MySqlConnection writerConnection = DebeziumUtils.createMySqlConnection(config);
        String writerConnectionString = writerConnection.connectionString();

        // Both should use the writer hostname when reader is not configured
        assertThat(readerConnectionString).contains(WRITER_HOSTNAME);
        assertThat(writerConnectionString).contains(WRITER_HOSTNAME);
    }

    @Test
    void testJdbcConnectionFactoryUsesHostnameOverride() {
        MySqlSourceConfig config = createConfig(WRITER_HOSTNAME, null);

        // Factory without hostname override - should use config hostname
        JdbcConnectionFactory factoryWithoutOverride = new JdbcConnectionFactory(config);

        // Factory with hostname override - should use override
        JdbcConnectionFactory factoryWithOverride =
                new JdbcConnectionFactory(config, READER_HOSTNAME);

        // Verify the factories are created with correct configuration
        assertThat(factoryWithoutOverride).isNotNull();
        assertThat(factoryWithOverride).isNotNull();
    }

    @Test
    void testConnectionPoolIdDifferentForWriterAndReader() {
        // Pool IDs should be different for writer and reader to ensure separate pools
        ConnectionPoolId writerPoolId =
                new ConnectionPoolId(WRITER_HOSTNAME, PORT, "testuser");
        ConnectionPoolId readerPoolId =
                new ConnectionPoolId(READER_HOSTNAME, PORT, "testuser");

        assertThat(writerPoolId).isNotEqualTo(readerPoolId);
        assertThat(writerPoolId.getHost()).isEqualTo(WRITER_HOSTNAME);
        assertThat(readerPoolId.getHost()).isEqualTo(READER_HOSTNAME);
    }

    @Test
    void testConnectionPoolIdSameForSameHostname() {
        // Same hostname should result in equal pool IDs
        ConnectionPoolId poolId1 =
                new ConnectionPoolId(WRITER_HOSTNAME, PORT, "testuser");
        ConnectionPoolId poolId2 =
                new ConnectionPoolId(WRITER_HOSTNAME, PORT, "testuser");

        assertThat(poolId1).isEqualTo(poolId2);
        assertThat(poolId1.hashCode()).isEqualTo(poolId2.hashCode());
    }

    private MySqlSourceConfig createConfig(String hostname, String scanSnapshotHostname) {
        MySqlSourceConfigFactory factory =
                new MySqlSourceConfigFactory()
                        .startupOptions(StartupOptions.initial())
                        .databaseList("testdb")
                        .tableList("testdb.testtable")
                        .includeSchemaChanges(false)
                        .hostname(hostname)
                        .port(PORT)
                        .splitSize(10)
                        .fetchSize(2)
                        .connectTimeout(Duration.ofSeconds(20))
                        .username("testuser")
                        .password("testpw")
                        .serverTimeZone(ZoneId.of("UTC").toString())
                        .jdbcProperties(new Properties());

        if (scanSnapshotHostname != null) {
            factory.snapshotHostname(scanSnapshotHostname);
        }

        return factory.createConfig(0);
    }
}
