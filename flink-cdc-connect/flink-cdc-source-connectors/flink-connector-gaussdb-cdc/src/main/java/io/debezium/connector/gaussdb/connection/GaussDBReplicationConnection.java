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

package io.debezium.connector.gaussdb.connection;

import com.huawei.gaussdb.jdbc.PGConnection;
import com.huawei.gaussdb.jdbc.replication.PGReplicationStream;
import com.huawei.gaussdb.jdbc.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.connector.gaussdb.GaussDBConnectorConfig;
import io.debezium.connector.gaussdb.decoder.MppdbDecodingMessageDecoder;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.connector.postgresql.connection.WalPositionLocator;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A lightweight replication connection for GaussDB.
 *
 * <p>GaussDB exposes a PostgreSQL-compatible logical replication protocol; this class uses the
 * PostgreSQL JDBC driver replication API and Debezium's {@link ReplicationStream} contract.
 */
public class GaussDBReplicationConnection implements ReplicationConnection {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBReplicationConnection.class);

    private static final String GAUSSDB_JDBC_URL_PATTERN = "jdbc:gaussdb://%s:%d/%s";

    private final GaussDBConnectorConfig connectorConfig;
    private final String slotName;
    private final String pluginName;
    private final boolean dropSlotOnClose;
    private final Duration statusUpdateInterval;
    private final MppdbDecodingMessageDecoder messageDecoder;
    private final TypeRegistry typeRegistry;

    private volatile Connection connection;

    public GaussDBReplicationConnection(
            GaussDBConnectorConfig connectorConfig,
            String slotName,
            String pluginName,
            boolean dropSlotOnClose,
            Duration statusUpdateInterval,
            MppdbDecodingMessageDecoder messageDecoder,
            TypeRegistry typeRegistry,
            Connection initialConnection) {
        this.connectorConfig =
                Objects.requireNonNull(connectorConfig, "connectorConfig must not be null");
        this.slotName = Objects.requireNonNull(slotName, "slotName must not be null");
        this.pluginName = Objects.requireNonNull(pluginName, "pluginName must not be null");
        this.dropSlotOnClose = dropSlotOnClose;
        this.statusUpdateInterval = statusUpdateInterval;
        this.messageDecoder =
                Objects.requireNonNull(messageDecoder, "messageDecoder must not be null");
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry must not be null");
        // Do not use the passed-in connection: it may be created by GaussDB JDBC driver which
        // does not support unwrap(), while this class requires PostgreSQL JDBC driver's replication
        // API.
        // Let connectIfNeeded() create a PostgreSQL JDBC connection via
        // openReplicationConnection().
        this.connection = null;
    }

    @Override
    public synchronized void initConnection() throws SQLException, InterruptedException {
        connectIfNeeded();
        // Ensure the slot exists. If it already exists, ignore the error.
        try {
            createReplicationSlot();
        } catch (SQLException e) {
            if (isSlotAlreadyExists(e)) {
                LOG.debug("Replication slot '{}' already exists, skipping creation", slotName);
            } else {
                throw e;
            }
        }
    }

    @Override
    public synchronized ReplicationStream startStreaming(WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        return startStreaming(null, walPosition);
    }

    @Override
    public synchronized ReplicationStream startStreaming(Lsn offset, WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        initConnection();
        final Lsn startLsn = offset != null && offset.isValid() ? offset : null;
        final PGReplicationStream stream = startPgReplicationStream(startLsn);
        return new ReplicationStream() {
            private static final int CHECK_WARNINGS_AFTER_COUNT = 100;

            private int warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
            private ExecutorService keepAliveExecutor = null;
            private AtomicBoolean keepAliveRunning;
            private final Metronome metronome =
                    Metronome.sleeper(
                            statusUpdateInterval != null
                                    ? statusUpdateInterval
                                    : Duration.ofSeconds(10),
                            Clock.SYSTEM);

            private volatile Lsn lastReceivedLsn;

            @Override
            public void read(ReplicationMessageProcessor processor)
                    throws SQLException, InterruptedException {
                processWarnings(false);
                ByteBuffer read = stream.read();
                final Lsn lastReceiveLsn = convertToLsn(stream.getLastReceiveLSN());
                if (messageDecoder.shouldMessageBeSkipped(
                        read, lastReceiveLsn, startLsn, walPosition)) {
                    return;
                }
                deserializeMessages(read, processor);
            }

            @Override
            public boolean readPending(ReplicationMessageProcessor processor)
                    throws SQLException, InterruptedException {
                processWarnings(false);
                ByteBuffer read = stream.readPending();
                final Lsn lastReceiveLsn = convertToLsn(stream.getLastReceiveLSN());
                if (read == null) {
                    return false;
                }
                if (messageDecoder.shouldMessageBeSkipped(
                        read, lastReceiveLsn, startLsn, walPosition)) {
                    return true;
                }
                deserializeMessages(read, processor);
                return true;
            }

            private void deserializeMessages(
                    ByteBuffer buffer, ReplicationMessageProcessor processor)
                    throws SQLException, InterruptedException {
                lastReceivedLsn = convertToLsn(stream.getLastReceiveLSN());
                messageDecoder.processMessage(buffer, processor, typeRegistry);
            }

            @Override
            public void flushLsn(Lsn lsn) throws SQLException {
                stream.setFlushedLSN(convertToGaussDBLsn(lsn));
                stream.setAppliedLSN(convertToGaussDBLsn(lsn));
                stream.forceUpdateStatus();
            }

            @Override
            public Lsn lastReceivedLsn() {
                return lastReceivedLsn;
            }

            @Override
            public Lsn startLsn() {
                return startLsn;
            }

            @Override
            public void startKeepAlive(ExecutorService service) {
                if (keepAliveExecutor == null) {
                    keepAliveExecutor = service;
                    keepAliveRunning = new AtomicBoolean(true);
                    keepAliveExecutor.submit(
                            () -> {
                                while (keepAliveRunning.get()) {
                                    try {
                                        stream.forceUpdateStatus();
                                        metronome.pause();
                                    } catch (Exception exp) {
                                        throw new RuntimeException(
                                                "Unexpected exception while keeping replication stream alive",
                                                exp);
                                    }
                                }
                            });
                }
            }

            @Override
            public void stopKeepAlive() {
                if (keepAliveExecutor != null) {
                    keepAliveRunning.set(false);
                    keepAliveExecutor.shutdownNow();
                    keepAliveExecutor = null;
                }
            }

            @Override
            public void close() throws Exception {
                processWarnings(true);
                stream.close();
            }

            private void processWarnings(final boolean forced) throws SQLException {
                if (--warningCheckCounter == 0 || forced) {
                    warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
                    final Connection conn = connection;
                    if (conn == null) {
                        return;
                    }
                    for (SQLWarning w = conn.getWarnings(); w != null; w = w.getNextWarning()) {
                        LOG.debug(
                                "Server-side message: '{}', state = {}, code = {}",
                                w.getMessage(),
                                w.getSQLState(),
                                w.getErrorCode());
                    }
                    conn.clearWarnings();
                }
            }
        };
    }

    @Override
    public synchronized java.util.Optional<io.debezium.connector.postgresql.spi.SlotCreationResult>
            createReplicationSlot() throws SQLException {
        try {
            connectIfNeeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while creating replication slot", e);
        }
        final Connection conn = connection;
        if (conn == null) {
            throw new SQLException("Replication connection is not initialized");
        }
        try (Statement statement = conn.createStatement()) {
            try {
                final String createCommand =
                        String.format(
                                "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s", slotName, pluginName);
                LOG.info("Creating replication slot with command {}", createCommand);
                statement.execute(createCommand);
            } catch (SQLException first) {
                if (isSlotAlreadyExists(first)) {
                    throw first;
                }
                // Fallback to PostgreSQL-compatible function if supported.
                try (ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT * FROM pg_create_logical_replication_slot('%s', '%s')",
                                        slotName, pluginName))) {
                    if (!rs.next()) {
                        throw first;
                    }
                } catch (SQLException second) {
                    first.addSuppressed(second);
                    throw first;
                }
            }
        }
        return java.util.Optional.empty();
    }

    @Override
    public synchronized boolean isConnected() throws SQLException {
        final Connection c = connection;
        return c != null && !c.isClosed();
    }

    @Override
    public synchronized void reconnect() throws SQLException {
        close(false);
        try {
            connectIfNeeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            SQLException interrupted =
                    new SQLException("Interrupted while reconnecting replication connection", e);
            throw interrupted;
        }
    }

    @Override
    public synchronized void close() {
        close(true);
    }

    public synchronized void close(boolean dropSlot) {
        try {
            messageDecoder.close();
        } catch (Throwable t) {
            LOG.debug("Failed to close GaussDB message decoder", t);
        }

        if (dropSlotOnClose && dropSlot) {
            try {
                dropReplicationSlot();
            } catch (Throwable t) {
                LOG.warn("Failed to drop replication slot '{}'", slotName, t);
            }
        }

        final Connection c = connection;
        connection = null;
        if (c != null) {
            try {
                c.close();
            } catch (Throwable t) {
                LOG.debug("Failed to close replication JDBC connection", t);
            }
        }
    }

    private void dropReplicationSlot() throws SQLException {
        try {
            connectIfNeeded();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while dropping replication slot", e);
        }
        final Connection conn = connection;
        if (conn == null) {
            throw new SQLException("Replication connection is not initialized");
        }
        try (Statement statement = conn.createStatement()) {
            try {
                final String dropCommand = String.format("DROP_REPLICATION_SLOT \"%s\"", slotName);
                LOG.info("Dropping replication slot with command {}", dropCommand);
                statement.execute(dropCommand);
            } catch (SQLException first) {
                // Fallback to PostgreSQL-compatible function if supported.
                try {
                    statement.execute(
                            String.format("SELECT pg_drop_replication_slot('%s')", slotName));
                } catch (SQLException second) {
                    first.addSuppressed(second);
                    throw first;
                }
            }
        }
    }

    private void connectIfNeeded() throws SQLException, InterruptedException {
        if (connection != null && !connection.isClosed()) {
            return;
        }
        this.connection = openReplicationConnection(connectorConfig);
    }

    private PGReplicationStream startPgReplicationStream(Lsn startLsn) throws SQLException {
        final Connection c = this.connection;
        if (c == null) {
            throw new SQLException("Replication connection is not initialized");
        }
        final PGConnection pgConnection = c.unwrap(PGConnection.class);

        ChainedLogicalStreamBuilder builder =
                pgConnection
                        .getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName("\"" + slotName + "\"")
                        // GaussDB-specific options from official documentation
                        .withSlotOption("include-xids", false)
                        .withSlotOption("skip-empty-xacts", true);
        if (startLsn != null) {
            builder = builder.withStartPosition(convertToGaussDBLsn(startLsn));
        }
        messageDecoder.setContainsMetadata(false);

        if (statusUpdateInterval != null && statusUpdateInterval.toMillis() > 0) {
            builder.withStatusInterval(
                    Math.toIntExact(statusUpdateInterval.toMillis()), TimeUnit.MILLISECONDS);
        }

        final PGReplicationStream stream = builder.start();
        // ensure server sees feedback quickly
        stream.forceUpdateStatus();
        return stream;
    }

    private static boolean isSlotAlreadyExists(SQLException e) {
        final String msg = e.getMessage();
        return msg != null
                && (msg.contains("already exists")
                        || msg.contains("duplicate key value")
                        || msg.contains("replication slot") && msg.contains("exists"));
    }

    private static Connection openReplicationConnection(GaussDBConnectorConfig connectorConfig)
            throws SQLException, InterruptedException {
        final String host = connectorConfig.getJdbcConfig().getHostname();
        final int port = connectorConfig.getJdbcConfig().getPort();
        final String database = connectorConfig.getJdbcConfig().getDatabase();
        final String url = String.format(GAUSSDB_JDBC_URL_PATTERN, host, port, database);

        Properties props = new Properties();
        props.setProperty("user", connectorConfig.getJdbcConfig().getUser());
        props.setProperty("password", connectorConfig.getJdbcConfig().getPassword());
        props.setProperty("ApplicationName", "gaussdb-replication");
        // Required by GaussDB: assume PostgreSQL 9.4 compatibility
        props.setProperty("assumeMinServerVersion", "9.4");
        props.setProperty("replication", "database");
        props.setProperty("preferQueryMode", "simple");

        final Duration timeout =
                connectorConfig.connectionTimeout() != null
                        ? connectorConfig.connectionTimeout()
                        : Duration.ofSeconds(30);
        final int previousLoginTimeoutSeconds = DriverManager.getLoginTimeout();
        final int loginTimeoutSeconds =
                (int) Math.min(Integer.MAX_VALUE, Math.max(0L, timeout.toSeconds()));

        try {
            DriverManager.setLoginTimeout(loginTimeoutSeconds);
            return DriverManager.getConnection(url, props);
        } finally {
            DriverManager.setLoginTimeout(previousLoginTimeoutSeconds);
        }
    }

    /**
     * Converts GaussDB LogSequenceNumber to Debezium Lsn.
     *
     * @param gaussDBLsn the GaussDB LogSequenceNumber
     * @return the equivalent Debezium Lsn
     */
    private static Lsn convertToLsn(
            com.huawei.gaussdb.jdbc.replication.LogSequenceNumber gaussDBLsn) {
        if (gaussDBLsn == null) {
            return Lsn.INVALID_LSN;
        }
        return Lsn.valueOf(gaussDBLsn.asLong());
    }

    /**
     * Converts Debezium Lsn to GaussDB LogSequenceNumber.
     *
     * @param lsn the Debezium Lsn
     * @return the equivalent GaussDB LogSequenceNumber
     */
    private static com.huawei.gaussdb.jdbc.replication.LogSequenceNumber convertToGaussDBLsn(
            Lsn lsn) {
        if (lsn == null || !lsn.isValid()) {
            return com.huawei.gaussdb.jdbc.replication.LogSequenceNumber.INVALID_LSN;
        }
        return com.huawei.gaussdb.jdbc.replication.LogSequenceNumber.valueOf(lsn.asLong());
    }
}
