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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.handler.OracleSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils;
import org.apache.flink.cdc.connectors.oracle.util.ChunkUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleChangeEventSourceMetricsFactory;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleErrorHandler;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;
import static org.apache.flink.cdc.connectors.oracle.util.ChunkUtils.getChunkKeyColumn;

/** The context for fetch task that fetching data of snapshot split from Oracle data source. */
public class OracleSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceFetchTaskContext.class);

    private final OracleConnection connection;
    private final OracleEventMetadataProvider metadataProvider;

    private OracleDatabaseSchema databaseSchema;
    private OracleTaskContext taskContext;
    private OracleOffsetContext offsetContext;
    private OraclePartition partition;

    private SnapshotChangeEventSourceMetrics<OraclePartition> snapshotChangeEventSourceMetrics;
    private OracleStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<OraclePartition> dispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private OracleErrorHandler errorHandler;

    public OracleSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.connection = createOracleConnection(sourceConfig.getDbzConfiguration());
        this.metadataProvider = new OracleEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // initial stateful objects
        final OracleConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        EmbeddedFlinkDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                sourceSplitBase.getTableSchemas().values());
        this.databaseSchema = OracleUtils.createOracleDatabaseSchema(connectorConfig, connection);
        // todo logMiner or xStream
        this.offsetContext =
                loadStartingOffsetState(
                        new LogMinerOracleOffsetContextLoader(connectorConfig), sourceSplitBase);
        this.partition = new OraclePartition(connectorConfig.getLogicalName());
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new OracleTaskContext(connectorConfig, databaseSchema);
        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? getSourceConfig().getSplitSize()
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "oracle-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        new OracleSchemaChangeEventHandler());

        final OracleChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new OracleChangeEventSourceMetricsFactory(
                        new OracleStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider, connectorConfig));
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                (OracleStreamingChangeEventSourceMetrics)
                        changeEventSourceMetricsFactory.getStreamingMetrics(
                                taskContext, queue, metadataProvider);
        this.errorHandler = new OracleErrorHandler(connectorConfig, queue);
    }

    @Override
    public OracleSourceConfig getSourceConfig() {
        return (OracleSourceConfig) sourceConfig;
    }

    public OracleConnection getConnection() {
        return connection;
    }

    @Override
    public OracleConnectorConfig getDbzConnectorConfig() {
        return (OracleConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public OracleOffsetContext getOffsetContext() {
        return offsetContext;
    }

    public SnapshotChangeEventSourceMetrics<OraclePartition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public OracleStreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public OracleDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        OracleSourceConfig oracleSourceConfig = getSourceConfig();
        return ChunkUtils.getSplitType(
                getChunkKeyColumn(table, oracleSourceConfig.getChunkKeyColumn()));
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        RowType splitKeyType =
                getSplitType(getDatabaseSchema().tableFor(SourceRecordUtils.getTableId(record)));

        // RowId is chunk key column by default, compare RowId
        if (splitKeyType.getFieldNames().contains(ROWID.class.getSimpleName())) {
            ConnectHeaders headers = (ConnectHeaders) record.headers();
            ROWID rowId = null;
            try {
                rowId = new ROWID(headers.iterator().next().value().toString());
            } catch (SQLException e) {
                LOG.error("{} can not convert to RowId", record);
            }
            Object[] rowIds = new ROWID[] {rowId};
            return SourceRecordUtils.splitKeyRangeContains(rowIds, splitStart, splitEnd);
        } else {
            // config chunk key column compare
            Object[] key =
                    SourceRecordUtils.getSplitKey(splitKeyType, record, getSchemaNameAdjuster());
            return SourceRecordUtils.splitKeyRangeContains(key, splitStart, splitEnd);
        }
    }

    @Override
    public JdbcSourceEventDispatcher<OraclePartition> getEventDispatcher() {
        return dispatcher;
    }

    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        return dispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    public OraclePartition getPartition() {
        return partition;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return OracleUtils.getRedoLogPosition(sourceRecord);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private OracleOffsetContext loadStartingOffsetState(
            OffsetContext.Loader<OracleOffsetContext> loader, SourceSplitBase oracleSplit) {
        Offset offset =
                oracleSplit.isSnapshotSplit()
                        ? RedoLogOffset.INITIAL_OFFSET
                        : oracleSplit.asStreamSplit().getStartingOffset();

        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(
            OracleOffsetContext offset, OracleDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(Offsets.of(partition, offset));
    }

    /** Copied from debezium for accessing here. */
    public static class OracleEventMetadataProvider implements EventMetadataProvider {
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
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
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
            final String scn = sourceInfo.getString(SourceInfo.SCN_KEY);
            return Collect.hashMapOf(SourceInfo.SCN_KEY, scn == null ? "null" : scn);
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return sourceInfo.getString(SourceInfo.TXID_KEY);
        }
    }
}
