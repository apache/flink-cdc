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

package com.ververica.cdc.connectors.mysql.debezium.task.context;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl;
import com.ververica.cdc.connectors.mysql.debezium.dispatcher.SignalEventDispatcher;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.MySqlChangeEventSourceMetricsFactory;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset.BINLOG_FILENAME_OFFSET_KEY;
import static com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetUtils.initializeEffectiveOffset;

/**
 * A stateful task context that contains entries the debezium mysql connector task required.
 *
 * <p>The offset change and schema change should record to MySqlSplitState when emit the record,
 * thus the Flink's state mechanism can help to store/restore when failover happens.
 */
public class StatefulTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulTaskContext.class);
    private static final int DEFAULT_BINLOG_QUEUE_SIZE_IN_SNAPSHOT_SCAN = 1024;
    private static final Clock clock = Clock.SYSTEM;

    private final MySqlSourceConfig sourceConfig;
    private final MySqlConnectorConfig connectorConfig;
    private final MySqlEventMetadataProvider metadataProvider;
    private final SchemaNameAdjuster schemaNameAdjuster;
    private final MySqlConnection connection;
    private final BinaryLogClient binaryLogClient;

    private MySqlDatabaseSchema databaseSchema;
    private MySqlTaskContextImpl taskContext;
    private MySqlOffsetContext offsetContext;
    private TopicSelector<TableId> topicSelector;
    private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;
    private StreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
    private EventDispatcherImpl<TableId> dispatcher;
    private EventDispatcher.SnapshotReceiver snapshotReceiver;
    private SignalEventDispatcher signalEventDispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;

    public StatefulTaskContext(
            MySqlSourceConfig sourceConfig,
            BinaryLogClient binaryLogClient,
            MySqlConnection connection) {
        this.sourceConfig = sourceConfig;
        this.connectorConfig = sourceConfig.getMySqlConnectorConfig();
        this.schemaNameAdjuster = SchemaNameAdjuster.create();
        this.metadataProvider = new MySqlEventMetadataProvider();
        this.binaryLogClient = binaryLogClient;
        this.connection = connection;
    }

    public void configure(MySqlSplit mySqlSplit) {
        // initial stateful objects
        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        this.topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);
        EmbeddedFlinkDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                mySqlSplit.getTableSchemas().values());
        this.databaseSchema =
                DebeziumUtils.createMySqlDatabaseSchema(connectorConfig, tableIdCaseInsensitive);
        this.offsetContext =
                loadStartingOffsetState(new MySqlOffsetContext.Loader(connectorConfig), mySqlSplit);
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext =
                new MySqlTaskContextImpl(connectorConfig, databaseSchema, binaryLogClient);

        final int queueSize =
                mySqlSplit.isSnapshotSplit()
                        ? sourceConfig.getSplitSize() + DEFAULT_BINLOG_QUEUE_SIZE_IN_SNAPSHOT_SCAN
                        : connectorConfig.getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "mysql-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new EventDispatcherImpl<>(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        this.snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();

        this.signalEventDispatcher =
                new SignalEventDispatcher(
                        offsetContext.getPartition(), topicSelector.getPrimaryTopic(), queue);

        final MySqlChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new MySqlChangeEventSourceMetricsFactory(
                        new MySqlStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider));
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getStreamingMetrics(
                        taskContext, queue, metadataProvider);
        this.errorHandler =
                new MySqlErrorHandler(
                        connectorConfig.getLogicalName(), queue, taskContext, sourceConfig);
    }

    private void validateAndLoadDatabaseHistory(
            MySqlOffsetContext offset, MySqlDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(offset);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private MySqlOffsetContext loadStartingOffsetState(
            OffsetContext.Loader<MySqlOffsetContext> loader, MySqlSplit mySqlSplit) {
        BinlogOffset offset =
                mySqlSplit.isSnapshotSplit()
                        ? BinlogOffset.ofEarliest()
                        : initializeEffectiveOffset(
                                mySqlSplit.asBinlogSplit().getStartingOffset(), connection);

        LOG.info("Starting offset is initialized to {}", offset);

        MySqlOffsetContext mySqlOffsetContext = loader.load(offset.getOffset());

        if (!isBinlogAvailable(mySqlOffsetContext)) {
            throw new IllegalStateException(
                    "The connector is trying to read binlog starting at "
                            + mySqlOffsetContext.getSourceInfo()
                            + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
        }
        return mySqlOffsetContext;
    }

    private boolean isBinlogAvailable(MySqlOffsetContext offset) {
        String gtidStr = offset.gtidSet();
        if (gtidStr != null) {
            return checkGtidSet(offset);
        }

        return checkBinlogFilename(offset);
    }

    private boolean checkGtidSet(MySqlOffsetContext offset) {
        String gtidStr = offset.gtidSet();

        if (gtidStr.trim().isEmpty()) {
            return true; // start at beginning ...
        }

        String availableGtidStr = connection.knownGtidSet();
        if (availableGtidStr == null || availableGtidStr.trim().isEmpty()) {
            // Last offsets had GTIDs but the server does not use them ...
            LOG.warn(
                    "Connector used GTIDs previously, but MySQL does not know of any GTIDs or they are not enabled");
            return false;
        }
        // GTIDs are enabled
        GtidSet gtidSet = new GtidSet(gtidStr);
        // Get the GTID set that is available in the server ...
        GtidSet availableGtidSet = new GtidSet(availableGtidStr);
        if (gtidSet.isContainedWithin(availableGtidSet)) {
            LOG.info(
                    "MySQL current GTID set {} does contain the GTID set {} required by the connector.",
                    availableGtidSet,
                    gtidSet);
            // The replication is concept of mysql master-slave replication protocol ...
            final GtidSet gtidSetToReplicate =
                    connection.subtractGtidSet(availableGtidSet, gtidSet);
            final GtidSet purgedGtidSet = connection.purgedGtidSet();
            LOG.info("Server has already purged {} GTIDs", purgedGtidSet);
            final GtidSet nonPurgedGtidSetToReplicate =
                    connection.subtractGtidSet(gtidSetToReplicate, purgedGtidSet);
            LOG.info(
                    "GTID set {} known by the server but not processed yet, for replication are available only GTID set {}",
                    gtidSetToReplicate,
                    nonPurgedGtidSetToReplicate);
            if (!gtidSetToReplicate.equals(nonPurgedGtidSetToReplicate)) {
                LOG.warn("Some of the GTIDs needed to replicate have been already purged");
                return false;
            }
            return true;
        }
        LOG.info("Connector last known GTIDs are {}, but MySQL has {}", gtidSet, availableGtidSet);
        return false;
    }

    private boolean checkBinlogFilename(MySqlOffsetContext offset) {
        String binlogFilename = offset.getSourceInfo().getString(BINLOG_FILENAME_OFFSET_KEY);
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }

        // Accumulate the available binlog filenames ...
        List<String> logNames = connection.availableBinlogFiles();

        // And compare with the one we're supposed to use ...
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if (!found) {
            LOG.info(
                    "Connector requires binlog file '{}', but MySQL only has {}",
                    binlogFilename,
                    String.join(", ", logNames));
        } else {
            LOG.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }
        return found;
    }

    /** Copied from debezium for accessing here. */
    public static class MySqlEventMetadataProvider implements EventMetadataProvider {
        public static final String SERVER_ID_KEY = "server_id";

        public static final String GTID_KEY = "gtid";
        public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
        public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
        public static final String BINLOG_ROW_IN_EVENT_OFFSET_KEY = "row";
        public static final String THREAD_KEY = "thread";
        public static final String QUERY_KEY = "query";

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return Collect.hashMapOf(
                    BINLOG_FILENAME_OFFSET_KEY,
                    sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY),
                    BINLOG_POSITION_OFFSET_KEY,
                    Long.toString(sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY)),
                    BINLOG_ROW_IN_EVENT_OFFSET_KEY,
                    Integer.toString(sourceInfo.getInt32(BINLOG_ROW_IN_EVENT_OFFSET_KEY)));
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            return ((MySqlOffsetContext) offset).getTransactionId();
        }
    }

    public static Clock getClock() {
        return clock;
    }

    public MySqlSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public MySqlConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public MySqlConnection getConnection() {
        return connection;
    }

    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }

    public MySqlDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    public MySqlTaskContextImpl getTaskContext() {
        return taskContext;
    }

    public EventDispatcherImpl<TableId> getDispatcher() {
        return dispatcher;
    }

    public EventDispatcher.SnapshotReceiver getSnapshotReceiver() {
        return snapshotReceiver;
    }

    public SignalEventDispatcher getSignalEventDispatcher() {
        return signalEventDispatcher;
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public MySqlOffsetContext getOffsetContext() {
        return offsetContext;
    }

    public TopicSelector<TableId> getTopicSelector() {
        return topicSelector;
    }

    public SnapshotChangeEventSourceMetrics getSnapshotChangeEventSourceMetrics() {
        snapshotChangeEventSourceMetrics.reset();
        return snapshotChangeEventSourceMetrics;
    }

    public StreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
        streamingChangeEventSourceMetrics.reset();
        return streamingChangeEventSourceMetrics;
    }

    public SchemaNameAdjuster getSchemaNameAdjuster() {
        return schemaNameAdjuster;
    }
}
