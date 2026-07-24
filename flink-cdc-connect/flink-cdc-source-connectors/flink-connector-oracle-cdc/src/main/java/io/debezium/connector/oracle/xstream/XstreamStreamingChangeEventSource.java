/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDatabaseVersion;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import oracle.sql.NUMBER;
import oracle.streams.StreamsException;
import oracle.streams.XStreamOut;
import oracle.streams.XStreamUtility;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Local override of Debezium's {@code XstreamStreamingChangeEventSource}.
 *
 * <p>Enhancement:
 *
 * <ul>
 *   <li>For each receive loop, we additionally inspect {@code XStreamOut#getFetchLowWatermark()}
 *       and advance {@code offsetContext} when possible. This lets bounded split logic evaluate
 *       ending offset even when no user-level DML/DDL event is emitted for captured tables.
 * </ul>
 */
public class XstreamStreamingChangeEventSource
        implements StreamingChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(XstreamStreamingChangeEventSource.class);
    static final int XSTREAM_ATTACH_MAX_RETRIES = 3;
    static final long XSTREAM_ATTACH_BASE_DELAY_MILLIS = 10000L;
    static final long XSTREAM_ATTACH_MAX_DELAY_MILLIS = 30000L;
    static final long XSTREAM_ATTACH_TOTAL_RETRY_WINDOW_MILLIS = 60000L;
    private static final String ACTIVE_XSTREAM_SESSION_ERROR_CODE = "ORA-26812";

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final EventDispatcher<OraclePartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleStreamingChangeEventSourceMetrics streamingMetrics;
    private final String xStreamServerName;
    private final Object xsOutLock = new Object();
    private volatile XStreamOut xsOut;
    private final int posVersion;
    private final Long startupTimestampMillis;
    private boolean reachedStartupTimestamp;
    private long startupFilteredCount;
    private boolean startupSeekStarted;

    /**
     * A message box between thread that is informed about committed offsets and the XStream thread.
     * When the last offset is committed its value is passed to the XStream thread and a watermark
     * is set to signal which events were safely processed.
     *
     * <p>This is important as setting watermark in a concurrent thread can lead to a deadlock due
     * to an internal Oracle code locking.
     */
    private final AtomicReference<PositionAndScn> lcrMessage = new AtomicReference<>();

    public XstreamStreamingChangeEventSource(
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            EventDispatcher<OraclePartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            OracleDatabaseSchema schema,
            OracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this(
                connectorConfig,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics,
                null);
    }

    public XstreamStreamingChangeEventSource(
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            EventDispatcher<OraclePartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            OracleDatabaseSchema schema,
            OracleStreamingChangeEventSourceMetrics streamingMetrics,
            Long startupTimestampMillis) {
        this(
                connectorConfig,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                streamingMetrics,
                startupTimestampMillis,
                resolvePosVersion(jdbcConnection, connectorConfig));
    }

    XstreamStreamingChangeEventSource(
            OracleConnectorConfig connectorConfig,
            OracleConnection jdbcConnection,
            EventDispatcher<OraclePartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            OracleDatabaseSchema schema,
            OracleStreamingChangeEventSourceMetrics streamingMetrics,
            Long startupTimestampMillis,
            int posVersion) {
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
        this.xStreamServerName = connectorConfig.getXoutServerName();
        this.posVersion = posVersion;
        this.startupTimestampMillis = startupTimestampMillis;
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            OraclePartition partition,
            OracleOffsetContext offsetContext)
            throws InterruptedException {

        LcrEventHandler eventHandler =
                new LcrEventHandler(
                        connectorConfig,
                        errorHandler,
                        dispatcher,
                        clock,
                        schema,
                        partition,
                        offsetContext,
                        TableNameCaseSensitivity.INSENSITIVE.equals(
                                connectorConfig
                                        .getAdapter()
                                        .getTableNameCaseSensitivity(jdbcConnection)),
                        this,
                        streamingMetrics);

        try (OracleConnection xsConnection =
                new OracleConnection(jdbcConnection.config(), () -> getClass().getClassLoader())) {
            try {
                // 1. connect
                final byte[] startPosition;
                String lcrPosition = offsetContext.getLcrPosition();
                if (lcrPosition != null) {
                    startPosition = LcrPosition.valueOf(lcrPosition).getRawPosition();
                } else {
                    startPosition = convertScnToPosition(offsetContext.getScn());
                }

                XStreamOut attachedXStream =
                        retryWhenSessionBusy(
                                context,
                                xStreamServerName,
                                () -> attachXStream(xsConnection, startPosition),
                                this::pauseBetweenAttachRetries,
                                getAttachBusyRetryTimes());
                if (attachedXStream == null) {
                    LOGGER.info(
                            "Skip attaching XStream outbound server {} because source is stopping.",
                            xStreamServerName);
                    return;
                }
                setXsOut(attachedXStream);

                // 2. receive events while running
                while (context.isRunning()) {
                    LOGGER.trace("Receiving LCR");
                    xsOut.receiveLCRCallback(eventHandler, XStreamOut.DEFAULT_MODE);
                    advanceOffsetByFetchLowWatermark(offsetContext);
                    onAfterReceive(context, partition, offsetContext);
                }
            } finally {
                // 3. disconnect
                detachXStreamSession();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!context.isRunning()) {
                LOGGER.info(
                        "Ignore XStream interruption while stopping outbound server {}.",
                        xStreamServerName);
                return;
            }
            errorHandler.setProducerThrowable(e);
        } catch (Throwable e) {
            if (!context.isRunning()) {
                LOGGER.info(
                        "Ignore XStream exception while stopping outbound server {}.",
                        xStreamServerName,
                        e);
                return;
            }
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (xsOut != null) {
            LOGGER.debug("Sending message to request recording of offsets to Oracle");
            final LcrPosition lcrPosition =
                    LcrPosition.valueOf((String) offset.get(SourceInfo.LCR_POSITION_KEY));
            final Scn scn =
                    OracleOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
            // We can safely overwrite the message even if it was not processed. The watermarked
            // will be set to the highest (last) delivered value in a single step instead of
            // incrementally.
            sendPublishedPosition(lcrPosition, scn);
        }
    }

    private void advanceOffsetByFetchLowWatermark(OracleOffsetContext offsetContext) {
        if (xsOut == null) {
            return;
        }
        final byte[] fetchLowWatermark = xsOut.getFetchLowWatermark();
        if (fetchLowWatermark == null || fetchLowWatermark.length == 0) {
            return;
        }
        try {
            final LcrPosition fetchPosition = new LcrPosition(fetchLowWatermark);
            final LcrPosition currentPosition = LcrPosition.valueOf(offsetContext.getLcrPosition());
            if (currentPosition != null && fetchPosition.compareTo(currentPosition) <= 0) {
                return;
            }
            offsetContext.setScn(fetchPosition.getScn());
            offsetContext.setEventScn(fetchPosition.getScn());
            offsetContext.setLcrPosition(fetchPosition.toString());
            LOGGER.debug(
                    "Advanced XStream offset by fetch low watermark to {}", fetchPosition.getScn());
        } catch (RuntimeException e) {
            // Keep original behaviour (continue receiving). Bounded split fallback still relies on
            // event-driven SCN update path in LcrEventHandler.
            LOGGER.warn(
                    "Failed to parse XStream fetch low watermark, skip proactive offset update.",
                    e);
        }
    }

    protected void onAfterReceive(
            ChangeEventSourceContext context,
            OraclePartition partition,
            OracleOffsetContext offsetContext)
            throws InterruptedException {
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    private byte[] convertScnToPosition(Scn scn) {
        try {
            return XStreamUtility.convertSCNToPosition(
                    new NUMBER(scn.toString(), 0), this.posVersion);
        } catch (SQLException | StreamsException e) {
            throw new RuntimeException(e);
        }
    }

    public static class PositionAndScn {
        public final LcrPosition position;
        public final byte[] scn;

        public PositionAndScn(LcrPosition position, byte[] scn) {
            this.position = position;
            this.scn = scn;
        }
    }

    XStreamOut getXsOut() {
        return xsOut;
    }

    public void closeXStreamSession() {
        detachXStreamSession();
    }

    private void sendPublishedPosition(final LcrPosition lcrPosition, final Scn scn) {
        lcrMessage.set(
                new PositionAndScn(lcrPosition, (scn != null) ? convertScnToPosition(scn) : null));
    }

    PositionAndScn receivePublishedPosition() {
        return lcrMessage.getAndSet(null);
    }

    boolean shouldSkipDataEvent(OracleOffsetContext offsetContext) {
        if (startupTimestampMillis == null || reachedStartupTimestamp) {
            return false;
        }
        if (!startupSeekStarted) {
            startupSeekStarted = true;
            LOGGER.info(
                    "Begin to seek Oracle xstream to startup timestamp {}.",
                    startupTimestampMillis);
        }
        Struct sourceInfo = offsetContext.getSourceInfo();
        Long eventTimestamp =
                sourceInfo == null ? null : sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        if (eventTimestamp != null && eventTimestamp >= startupTimestampMillis) {
            reachedStartupTimestamp = true;
            LOGGER.info(
                    "Successfully seek Oracle xstream to startup timestamp {} with filtered {} change events.",
                    startupTimestampMillis,
                    startupFilteredCount);
            return false;
        }
        startupFilteredCount++;
        if (startupFilteredCount % 10000 == 0) {
            LOGGER.info(
                    "Seeking Oracle xstream to startup timestamp {} with filtered {} change events.",
                    startupTimestampMillis,
                    startupFilteredCount);
        }
        return true;
    }

    private static int resolvePosVersion(
            OracleConnection connection, OracleConnectorConfig connectorConfig) {
        // Option 'internal.database.oracle.version' takes precedence
        final String oracleVersion = connectorConfig.getOracleVersion();
        if (!Strings.isNullOrEmpty(oracleVersion)) {
            if ("11".equals(oracleVersion)) {
                return XStreamUtility.POS_VERSION_V1;
            }
            return XStreamUtility.POS_VERSION_V2;
        }

        // As fallback, resolve this based on the OracleDatabaseVersion
        final OracleDatabaseVersion databaseVersion = connection.getOracleVersion();
        if (databaseVersion.getMajor() == 11
                || (databaseVersion.getMajor() == 12 && databaseVersion.getMaintenance() < 2)) {
            return XStreamUtility.POS_VERSION_V1;
        }
        return XStreamUtility.POS_VERSION_V2;
    }

    protected XStreamOut attachXStream(OracleConnection xsConnection, byte[] startPosition)
            throws StreamsException {
        try {
            return XStreamOut.attach(
                    (oracle.jdbc.OracleConnection) xsConnection.connection(),
                    xStreamServerName,
                    startPosition,
                    1,
                    1,
                    XStreamOut.DEFAULT_MODE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void pauseBetweenAttachRetries(long delayMillis) throws InterruptedException {
        Thread.sleep(delayMillis);
    }

    protected int getAttachBusyRetryTimes() {
        return XSTREAM_ATTACH_MAX_RETRIES;
    }

    private void setXsOut(XStreamOut xsOut) {
        synchronized (xsOutLock) {
            this.xsOut = xsOut;
        }
    }

    private void detachXStreamSession() {
        XStreamOut currentXsOut;
        synchronized (xsOutLock) {
            currentXsOut = xsOut;
            xsOut = null;
        }
        if (currentXsOut == null) {
            return;
        }
        try {
            currentXsOut.detach(XStreamOut.DEFAULT_MODE);
        } catch (StreamsException e) {
            LOGGER.error("Couldn't detach from XStream outbound server " + xStreamServerName, e);
        }
    }

    static <T> T retryWhenSessionBusy(
            ChangeEventSourceContext context,
            String xStreamServerName,
            XStreamAttachOperation<T> attachOperation,
            RetrySleeper retrySleeper,
            int maxRetries)
            throws StreamsException, InterruptedException {
        int retryAttempt = 0;
        while (true) {
            if (!context.isRunning()) {
                return null;
            }
            try {
                return attachOperation.attach();
            } catch (StreamsException e) {
                if (!isActiveSessionAttachedError(e)) {
                    throw e;
                }
                if (retryAttempt >= maxRetries) {
                    throw sessionBusyTimeoutException(xStreamServerName, maxRetries, e);
                }
                retryAttempt++;
                long delayMillis = getAttachRetryDelayMillis(retryAttempt);
                LOGGER.warn(
                        "XStream outbound server {} is still occupied by an active session ({}), retrying attach attempt {}/{} after {} ms.",
                        xStreamServerName,
                        ACTIVE_XSTREAM_SESSION_ERROR_CODE,
                        retryAttempt,
                        maxRetries,
                        delayMillis);
                retrySleeper.sleep(delayMillis);
            }
        }
    }

    static long getAttachRetryDelayMillis(int attempt) {
        long exponentialDelay = XSTREAM_ATTACH_BASE_DELAY_MILLIS << Math.max(0, attempt - 1);
        return Math.min(exponentialDelay, XSTREAM_ATTACH_MAX_DELAY_MILLIS);
    }

    private static StreamsException sessionBusyTimeoutException(
            String xStreamServerName, int maxRetries, StreamsException cause) {
        String detailMessage =
                maxRetries > 0
                        ? String.format(
                                "XStream outbound server \"%s\" is still occupied by an active session after %d retries (~%d ms). Please clean the residual session and retry.",
                                xStreamServerName,
                                maxRetries,
                                XSTREAM_ATTACH_TOTAL_RETRY_WINDOW_MILLIS)
                        : String.format(
                                "XStream outbound server \"%s\" is still occupied by an active session and retry is disabled for this path. Please clean the residual session and retry.",
                                xStreamServerName);
        StreamsException wrapped = new StreamsException(detailMessage);
        wrapped.initCause(cause);
        return wrapped;
    }

    static boolean isActiveSessionAttachedError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null
                    && current.getMessage().contains(ACTIVE_XSTREAM_SESSION_ERROR_CODE)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    @FunctionalInterface
    interface XStreamAttachOperation<T> {
        T attach() throws StreamsException;
    }

    @FunctionalInterface
    interface RetrySleeper {
        void sleep(long delayMillis) throws InterruptedException;
    }
}
