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

package org.apache.flink.cdc.connectors.mysql.debezium.task;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link MySqlSnapshotSplitReadTask} to verify correct usage of writer and reader
 * connections.
 *
 * <p>The key behavior being tested:
 *
 * <ul>
 *   <li>Writer connection is used for binlog position retrieval (SHOW MASTER STATUS)
 *   <li>Reader connection is used for snapshot data queries (SELECT statements)
 *   <li>When reader hostname is not configured, reader connection falls back to writer
 * </ul>
 */
class MySqlSnapshotSplitReadTaskConnectionTest {

    private static final String PRIMARY_HOSTNAME = "writer-host";
    private static final String SNAPSHOT_HOSTNAME = "reader-host";
    private static final int PORT = 3306;

    @Test
    void testStatefulTaskContextWithSeparateConnections() {
        MySqlSourceConfig config = createConfig(PRIMARY_HOSTNAME, SNAPSHOT_HOSTNAME);
        BinaryLogClient binaryLogClient = mock(BinaryLogClient.class);

        // Create separate writer and reader connections
        MySqlConnection writerConnection = DebeziumUtils.createMySqlConnection(config);
        MySqlConnection readerConnection = DebeziumUtils.createSnapshotMySqlConnection(config);

        // Create StatefulTaskContext with both connections
        StatefulTaskContext context =
                new StatefulTaskContext(
                        config, binaryLogClient, writerConnection, readerConnection);

        // Verify the context returns the correct connections
        assertThat(context.getConnection()).isSameAs(writerConnection);
        assertThat(context.getSnapshotConnection()).isSameAs(readerConnection);

        // Verify writer and reader connections point to different hosts
        assertThat(writerConnection.connectionString()).contains(PRIMARY_HOSTNAME);
        assertThat(readerConnection.connectionString()).contains(SNAPSHOT_HOSTNAME);
    }

    @Test
    void testStatefulTaskContextWithSameConnectionWhenNoReaderConfigured() {
        MySqlSourceConfig config = createConfig(PRIMARY_HOSTNAME, null);
        BinaryLogClient binaryLogClient = mock(BinaryLogClient.class);

        // When no reader hostname is configured, createReaderMySqlConnection falls back to writer
        MySqlConnection writerConnection = DebeziumUtils.createMySqlConnection(config);
        MySqlConnection readerConnection = DebeziumUtils.createSnapshotMySqlConnection(config);

        // Create StatefulTaskContext
        StatefulTaskContext context =
                new StatefulTaskContext(
                        config, binaryLogClient, writerConnection, readerConnection);

        // Verify both connections point to the same host (writer)
        assertThat(context.getConnection().connectionString()).contains(PRIMARY_HOSTNAME);
        assertThat(context.getSnapshotConnection().connectionString()).contains(PRIMARY_HOSTNAME);
    }

    @Test
    void testStatefulTaskContextLegacyConstructorUsesSameConnection() {
        MySqlSourceConfig config = createConfig(PRIMARY_HOSTNAME, SNAPSHOT_HOSTNAME);
        BinaryLogClient binaryLogClient = mock(BinaryLogClient.class);

        // Use the legacy constructor that takes only one connection
        MySqlConnection connection = DebeziumUtils.createMySqlConnection(config);
        StatefulTaskContext context = new StatefulTaskContext(config, binaryLogClient, connection);

        // Both getConnection() and getSnapshotConnection() should return the same instance
        assertThat(context.getConnection()).isSameAs(context.getSnapshotConnection());
    }

    @Test
    void testWriterConnectionUsedForBinlogPosition() {
        MySqlSourceConfig config = createConfig(PRIMARY_HOSTNAME, SNAPSHOT_HOSTNAME);

        // The writer connection should be used for binlog position queries
        // (SHOW MASTER STATUS only works on the primary/writer)
        MySqlConnection writerConnection = DebeziumUtils.createMySqlConnection(config);
        MySqlConnection readerConnection = DebeziumUtils.createSnapshotMySqlConnection(config);

        // Verify writer points to writer host (required for binlog operations)
        assertThat(writerConnection.connectionString()).contains(PRIMARY_HOSTNAME);
        assertThat(writerConnection.connectionString()).doesNotContain(SNAPSHOT_HOSTNAME);

        // Verify reader points to reader host (for snapshot data queries)
        assertThat(readerConnection.connectionString()).contains(SNAPSHOT_HOSTNAME);
        assertThat(readerConnection.connectionString()).doesNotContain(PRIMARY_HOSTNAME);
    }

    @Test
    void testReaderConnectionFallbackWhenReaderHostnameIsNull() {
        MySqlSourceConfig configWithoutReader = createConfig(PRIMARY_HOSTNAME, null);
        MySqlSourceConfig configWithReader = createConfig(PRIMARY_HOSTNAME, SNAPSHOT_HOSTNAME);

        // When scanSnapshotHostname is null, createReaderMySqlConnection should return a connection
        // that uses the primary hostname
        MySqlConnection readerConnectionWithoutConfig =
                DebeziumUtils.createSnapshotMySqlConnection(configWithoutReader);
        MySqlConnection readerConnectionWithConfig =
                DebeziumUtils.createSnapshotMySqlConnection(configWithReader);

        // Without reader configured: should use writer hostname
        assertThat(readerConnectionWithoutConfig.connectionString()).contains(PRIMARY_HOSTNAME);

        // With reader configured: should use reader hostname
        assertThat(readerConnectionWithConfig.connectionString()).contains(SNAPSHOT_HOSTNAME);
        assertThat(readerConnectionWithConfig.connectionString()).doesNotContain(PRIMARY_HOSTNAME);
    }

    /**
     * Verifies that when snapshotConnection and connection are different objects (replica
     * configured), both are closed independently.
     */
    @Test
    void testCloseWithSeparateConnectionsClosesBoth() throws Exception {
        MySqlSourceConfig config = createConfig(PRIMARY_HOSTNAME, SNAPSHOT_HOSTNAME);
        BinaryLogClient binaryLogClient = mock(BinaryLogClient.class);
        MySqlConnection primaryConnection = mock(MySqlConnection.class);
        MySqlConnection snapshotConnection = mock(MySqlConnection.class);

        StatefulTaskContext context =
                new StatefulTaskContext(
                        config, binaryLogClient, primaryConnection, snapshotConnection);
        context.close();

        verify(primaryConnection, times(1)).close();
        verify(snapshotConnection, times(1)).close();
    }

    /**
     * Verifies that when snapshotConnection and connection are the same object (no replica
     * configured), close() is called exactly once — not twice.
     */
    @Test
    void testCloseWithSharedConnectionClosesOnce() throws Exception {
        MySqlSourceConfig config = createConfig(PRIMARY_HOSTNAME, null);
        BinaryLogClient binaryLogClient = mock(BinaryLogClient.class);
        MySqlConnection sharedConnection = mock(MySqlConnection.class);

        // Legacy constructor passes the same object for both connection fields
        StatefulTaskContext context =
                new StatefulTaskContext(config, binaryLogClient, sharedConnection);
        context.close();

        verify(sharedConnection, times(1)).close();
    }

    private MySqlSourceConfig createConfig(String hostname, String snapshotHostname) {
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

        if (snapshotHostname != null) {
            factory.snapshotHostname(snapshotHostname);
        }

        return factory.createConfig(0);
    }
}
