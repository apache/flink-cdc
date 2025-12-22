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
 * <p>
 * GaussDB exposes a PostgreSQL-compatible logical replication protocol; this
 * class uses the
 * PostgreSQL JDBC driver replication API and Debezium's
 * {@link ReplicationStream} contract.
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

    // Ending position for bounded reads - when reached, stream returns NoopMessage
    // instead of closing
    private volatile Lsn endingPos;

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
        this.connectorConfig = Objects.requireNonNull(connectorConfig, "connectorConfig must not be null");
        this.slotName = Objects.requireNonNull(slotName, "slotName must not be null");
        this.pluginName = Objects.requireNonNull(pluginName, "pluginName must not be null");
        this.dropSlotOnClose = dropSlotOnClose;
        this.statusUpdateInterval = statusUpdateInterval;
        this.messageDecoder = Objects.requireNonNull(messageDecoder, "messageDecoder must not be null");
        this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry must not be null");
        // Do not use the passed-in connection: it may be created by GaussDB JDBC driver
        // which
        // does not support unwrap(), while this class requires PostgreSQL JDBC driver's
        // replication
        // API.
        // Let connectIfNeeded() create a PostgreSQL JDBC connection via
        // openReplicationConnection().
        this.connection = null;
        this.endingPos = null;
    }

    /**
     * Sets the ending position for bounded reads. When the stream reaches this LSN,
     * it will return a NoopMessage instead of requiring stream close/reopen.
     * This avoids slot contention issues with GaussDB.
     */
    public void setEndingPos(Lsn endingPos) {
        this.endingPos = endingPos;
        LOG.debug("Set ending position for bounded read: {}", endingPos);
    }

    public Lsn getEndingPos() {
        return this.endingPos;
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

        // GaussDB may take time to release slots after connection disruption.
        // Retry with exponential backoff to handle transient "Database connection
        // failed" errors.
        // This is based on GaussDB documentation recommendations for slot management.
        final int maxRetries = 3;
        final long initialDelayMs = 2000; // 2 seconds initial delay

        SQLException lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Ensure we have a fresh connection for retries
                if (attempt > 1) {
                    LOG.info("Retry attempt {} of {} for starting replication stream (slot: {})",
                            attempt, maxRetries, slotName);
                    // Force reconnect on retry
                    closeConnectionQuietly();
                    connectIfNeeded();
                }

                final PGReplicationStream stream = startPgReplicationStream(startLsn);
                if (attempt > 1) {
                    LOG.info("Successfully started replication stream on retry attempt {}", attempt);
                }
                return createReplicationStreamWrapper(stream, startLsn, walPosition);
            } catch (SQLException e) {
                lastException = e;
                String message = e.getMessage();
                boolean isRetryable = message != null && (message.contains("Database connection failed") ||
                        message.contains("EOF Exception") ||
                        message.contains("Connection reset") ||
                        message.contains("starting copy"));

                if (!isRetryable || attempt >= maxRetries) {
                    LOG.error("Failed to start replication stream after {} attempts: {}",
                            attempt, e.getMessage());
                    throw e;
                }

                long delayMs = initialDelayMs * (1L << (attempt - 1)); // Exponential backoff
                LOG.warn("Failed to start replication stream (attempt {}/{}): {}. Retrying in {}ms...",
                        attempt, maxRetries, e.getMessage(), delayMs);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while waiting to retry replication stream", ie);
                }
            }
        }

        // Should not reach here, but throw the last exception if we do
        throw lastException != null ? lastException
                : new SQLException("Failed to start replication stream after max retries");
    }

    private void closeConnectionQuietly() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.debug("Error closing connection during retry: {}", e.getMessage());
        } finally {
            connection = null;
        }
    }

    private ReplicationStream createReplicationStreamWrapper(PGReplicationStream stream, Lsn startLsn,
            WalPositionLocator walPosition) {
        return new ReplicationStream() {
            private static final int CHECK_WARNINGS_AFTER_COUNT = 100;

            private int warningCheckCounter = CHECK_WARNINGS_AFTER_COUNT;
            private ExecutorService keepAliveExecutor = null;
            private AtomicBoolean keepAliveRunning;
            private final Metronome metronome = Metronome.sleeper(
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

                // Check if we've reached the ending position for bounded reads
                if (reachEnd(lastReceiveLsn)) {
                    lastReceivedLsn = lastReceiveLsn;
                    LOG.trace("Reached ending position at LSN {}, returning NoopMessage", lastReceivedLsn);
                    // Process a no-op message to indicate we've reached the end
                    processor.process(null);
                    return;
                }

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

                // Check if we've reached the ending position for bounded reads
                if (reachEnd(lastReceiveLsn)) {
                    lastReceivedLsn = lastReceiveLsn;
                    LOG.trace("Reached ending position at LSN {}, returning NoopMessage", lastReceivedLsn);
                    // Process a no-op message to indicate we've reached the end
                    processor.process(null);
                    return true;
                }

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

            /**
             * Checks if the current LSN has reached or exceeded the ending position.
             * Used for bounded reads to stop streaming at a specific LSN.
             */
            private boolean reachEnd(Lsn receivedLsn) {
                if (receivedLsn == null) {
                    return false;
                }
                Lsn ending = endingPos;
                return ending != null && ending.compareTo(receivedLsn) <= 0;
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
                // GaussDB doesn't properly handle the PostgreSQL CopyBothResponse close
                // protocol.
                // When closing the replication stream, GaussDB may reset the connection,
                // causing EOFException or SocketException. We catch and log these errors
                // rather than propagating them, since the stream is already being closed.
                try {
                    stream.close();
                } catch (Exception e) {
                    LOG.warn(
                            "Error closing replication stream (expected with GaussDB): {}",
                            e.getMessage());
                    LOG.debug("Full exception during stream close", e);
                }
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
    public synchronized java.util.Optional<io.debezium.connector.postgresql.spi.SlotCreationResult> createReplicationSlot()
            throws SQLException {
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
                final String createCommand = String.format(
                        "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s", slotName, pluginName);
                LOG.info("Creating replication slot with command {}", createCommand);
                statement.execute(createCommand);
            } catch (SQLException first) {
                if (isSlotAlreadyExists(first)) {
                    throw first;
                }
                // Fallback to PostgreSQL-compatible function if supported.
                try (ResultSet rs = statement.executeQuery(
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
            SQLException interrupted = new SQLException("Interrupted while reconnecting replication connection", e);
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

        LOG.info(
                "Starting PG replication stream with slot '{}', startLsn={}, plugin={}",
                slotName,
                startLsn,
                pluginName);

        try {
            final PGConnection pgConnection = c.unwrap(PGConnection.class);

            ChainedLogicalStreamBuilder builder = pgConnection
                    .getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slotName) // GaussDB may not need extra quotes
                    // GaussDB-specific options from official documentation
                    .withSlotOption("include-xids", false)
                    .withSlotOption("skip-empty-xacts", true);
            if (startLsn != null) {
                builder = builder.withStartPosition(convertToGaussDBLsn(startLsn));
                LOG.info("Replication stream will start from LSN: {}", startLsn);
            } else {
                LOG.info("Replication stream will start from current WAL position");
            }
            messageDecoder.setContainsMetadata(false);

            if (statusUpdateInterval != null && statusUpdateInterval.toMillis() > 0) {
                builder.withStatusInterval(
                        Math.toIntExact(statusUpdateInterval.toMillis()), TimeUnit.MILLISECONDS);
            }

            LOG.info(\"Calling builder.start() to create replication stream...\");
            final PGReplicationStream stream = builder.start();
            LOG.info(\"Replication stream started successfully\");
            
            // Small delay to stabilize connection when connections are opened/closed in fast sequence
            // See reference implementation in GaussDB-For-Apache-Flink project
            try {
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            
            // ensure server sees feedback quickly
            stream.forceUpdateStatus();
            LOG.info(\"Initial status update sent to server\");
            return stream;
        } catch (SQLException e) {
            LOG.error(
                    "Failed to start replication stream for slot '{}': {}",
                    slotName,
                    e.getMessage(),
                    e);
            throw e;
        }
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
        // Disable SSL for replication connection to avoid protocol mismatch
        props.setProperty("sslmode", "disable");
        // Add TCP keepalive to prevent connection drops
        props.setProperty("tcpKeepAlive", "true");
        // Increase socket timeout for replication streams
        props.setProperty("socketTimeout", "0"); // 0 means infinite for replication
        // Set connection timeout
        props.setProperty("connectTimeout", "60");

        final Duration timeout = connectorConfig.connectionTimeout() != null
                ? connectorConfig.connectionTimeout()
                : Duration.ofSeconds(30);
        final int previousLoginTimeoutSeconds = DriverManager.getLoginTimeout();
        final int loginTimeoutSeconds = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, timeout.toSeconds()));

        LOG.info(
                "Opening replication connection to {}:{}/{} with user={}, replication={}, timeout={}s",
                host,
                port,
                database,
                props.getProperty("user"),
                props.getProperty("replication"),
                loginTimeoutSeconds);

        try {
            DriverManager.setLoginTimeout(loginTimeoutSeconds);
            Connection conn = DriverManager.getConnection(url, props);
            LOG.info("Replication connection established successfully");
            return conn;
        } catch (SQLException e) {
            LOG.error(
                    "Failed to establish replication connection to {}:{}/{}. Error: {}",
                    host,
                    port,
                    database,
                    e.getMessage(),
                    e);
            throw e;
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
