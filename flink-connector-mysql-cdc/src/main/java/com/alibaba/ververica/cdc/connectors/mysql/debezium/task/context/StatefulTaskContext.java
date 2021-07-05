/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context;

import org.apache.flink.configuration.Configuration;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.dispatcher.EventDispatcherImpl;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySQLSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlChangeEventSourceMetricsFactory;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlErrorHandler;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.debezium.config.CommonConnectorConfig.TOMBSTONES_ON_DELETE;

/**
 * A stateful task context that contains entries the debezium mysql connector task required.
 *
 * <p>The offset change and schema change should record to MySQLSplitState when emit the record,
 * thus the Flink's state mechanism can help to store/restore when failover happens.
 */
public class StatefulTaskContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulTaskContext.class);
    private static final Clock clock = Clock.SYSTEM;
    private static final ConcurrentMap<String, Collection<SchemaRecord>> SCHEMA_HISTORY =
            new ConcurrentHashMap<>();

    private final io.debezium.config.Configuration dezConf;
    private final MySqlConnectorConfig connectorConfig;
    private final MySqlEventMetadataProvider metadataProvider;
    private final SchemaNameAdjuster schemaNameAdjuster;
    private MySqlConnection connection;
    private BinaryLogClient binaryLogClient;

    private MySqlDatabaseSchema databaseSchema;
    private MySqlTaskContextImpl taskContext;
    private MySqlOffsetContext offsetContext;
    private TopicSelector<TableId> topicSelector;
    private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;
    private StreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
    private EventDispatcherImpl<TableId> dispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;

    public StatefulTaskContext(
            Configuration configuration,
            BinaryLogClient binaryLogClient,
            MySqlConnection connection) {
        this.dezConf = toDebeziumConfig(configuration);
        this.connectorConfig = new MySqlConnectorConfig(dezConf);
        this.schemaNameAdjuster = SchemaNameAdjuster.create();
        this.metadataProvider = new MySqlEventMetadataProvider();
        this.binaryLogClient = binaryLogClient;
        this.connection = connection;
    }

    public void configure(MySQLSplit mySQLSplit) {
        // initial stateful objects
        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        this.topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);
        final MySqlValueConverters valueConverters = getValueConverters(connectorConfig);
        SchemaStateUtils.registerHistory(
                dezConf.getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                mySQLSplit.getDatabaseHistory().values());
        this.databaseSchema =
                new MySqlDatabaseSchema(
                        connectorConfig,
                        valueConverters,
                        topicSelector,
                        schemaNameAdjuster,
                        tableIdCaseInsensitive);
        this.offsetContext =
                (MySqlOffsetContext)
                        loadOffsetState(new MySqlOffsetContext.Loader(connectorConfig), mySQLSplit);
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext =
                new MySqlTaskContextImpl(connectorConfig, databaseSchema, binaryLogClient);
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(connectorConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "mysql-cdc-connector-task"))
                        // no buffer any more, we use signal event
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
        this.errorHandler = new MySqlErrorHandler(connectorConfig.getLogicalName(), queue);
    }

    private void validateAndLoadDatabaseHistory(
            MySqlOffsetContext offset, MySqlDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(offset);
    }

    private boolean isBinlogAvailable(MySqlOffsetContext offset) {
        String binlogFilename = offset.getOffset().get("file").toString();
        if (binlogFilename == null) {
            return true; // start at current position
        }
        if (binlogFilename.equals("")) {
            return true; // start at beginning
        }
        final List<String> logNames = connection.availableBinlogFiles();
        boolean found = logNames.stream().anyMatch(binlogFilename::equals);
        if (!found) {
            LOGGER.info(
                    "Connector requires binlog file '{}', but MySQL only has {}",
                    binlogFilename,
                    String.join(", ", logNames));
        } else {
            LOGGER.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
        }
        return found;
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private OffsetContext loadOffsetState(OffsetContext.Loader loader, MySQLSplit mySQLSplit) {
        Map<String, Object> previousOffset = new HashMap<>();
        previousOffset.put("file", mySQLSplit.getOffset().getFilename());
        previousOffset.put("pos", mySQLSplit.getOffset().getPosition());
        if (previousOffset != null) {
            OffsetContext offsetContext = loader.load(previousOffset);
            return offsetContext;
        } else {
            return null;
        }
    }

    private static MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
        TemporalPrecisionMode timePrecisionMode = configuration.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = configuration.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                configuration
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                        bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        final boolean timeAdjusterEnabled =
                configuration.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                configuration.binaryHandlingMode(),
                timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
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

    public io.debezium.config.Configuration getDezConf() {
        return dezConf;
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

    /** Utils to get/put/remove the history of schema. */
    public static final class SchemaStateUtils {

        public static void registerHistory(
                String engineName, Collection<SchemaRecord> engineHistory) {
            SCHEMA_HISTORY.put(engineName, engineHistory);
        }

        public static Collection<SchemaRecord> retrieveHistory(String engineName) {
            return SCHEMA_HISTORY.getOrDefault(engineName, Collections.emptyList());
        }

        public static void removeHistory(String engineName) {
            SCHEMA_HISTORY.remove(engineName);
        }
    }

    // ------------ utils ---------
    public static BinaryLogClient getBinaryClient(Configuration configuration) {
        final MySqlConnectorConfig connectorConfig =
                new MySqlConnectorConfig(toDebeziumConfig(configuration));
        return new BinaryLogClient(
                connectorConfig.hostname(),
                connectorConfig.port(),
                connectorConfig.username(),
                connectorConfig.password());
    }

    public static MySqlConnection getConnection(Configuration configuration) {
        return new MySqlConnection(
                new MySqlConnection.MySqlConnectionConfiguration(toDebeziumConfig(configuration)));
    }

    public static MySqlDatabaseSchema getMySqlDatabaseSchema(
            Configuration configuration, MySqlConnection connection) {
        io.debezium.config.Configuration dezConf = toDebeziumConfig(configuration);
        MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(dezConf);
        boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        MySqlValueConverters valueConverters = getValueConverters(connectorConfig);
        return new MySqlDatabaseSchema(
                connectorConfig,
                valueConverters,
                topicSelector,
                schemaNameAdjuster,
                tableIdCaseInsensitive);
    }

    public static io.debezium.config.Configuration toDebeziumConfig(Configuration configuration) {
        return io.debezium.config.Configuration.from(configuration.toMap())
                .edit()
                .with(AbstractDatabaseHistory.INTERNAL_PREFER_DDL, true)
                .with(TOMBSTONES_ON_DELETE, false)
                .with("database.responseBuffering", "adaptive")
                .with(
                        "database.fetchSize",
                        configuration.getInteger(MySQLSourceOptions.SCAN_FETCH_SIZE))
                .build();
    }
}
