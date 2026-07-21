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
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkSchemaHistory;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.base.utils.SplitKeyUtils;
import org.apache.flink.cdc.connectors.oracle.connection.OracleSourceConnection;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.handler.OracleSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils;
import org.apache.flink.cdc.connectors.oracle.util.ChunkUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleChangeEventSourceMetricsFactory;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleErrorHandler;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerOracleOffsetContextLoader;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Collect;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;
import static org.apache.flink.cdc.connectors.oracle.util.ChunkUtils.getChunkKeyColumn;

/** The context for fetch task that fetching data of snapshot split from Oracle data source. */
public class OracleSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSourceFetchTaskContext.class);

    private final OracleSourceConnection connection;
    private final OracleEventMetadataProvider metadataProvider;

    private OracleDatabaseSchema databaseSchema;
    private OracleTaskContext taskContext;
    private OracleOffsetContext offsetContext;
    private OraclePartition partition;

    private SnapshotChangeEventSourceMetrics<OraclePartition> snapshotChangeEventSourceMetrics;
    private AbstractOracleStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;

    @SuppressWarnings("unchecked")
    private TopicNamingStrategy<TableId> topicNamingStrategy;

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
        @SuppressWarnings({"unchecked", "rawtypes"})
        TopicNamingStrategy<TableId> namingStrategy =
                (TopicNamingStrategy<TableId>)
                        (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig);
        this.topicNamingStrategy = namingStrategy;
        EmbeddedFlinkSchemaHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkSchemaHistory.DATABASE_HISTORY_INSTANCE_NAME),
                sourceSplitBase.getTableSchemas().values());
        // taskContext must be created before createOracleDatabaseSchema because
        // RelationalDatabaseSchema.buildAndRegisterSchema() calls taskContext.getRawConfig()
        // during schema recovery (Debezium 3.4.2 API change).
        this.taskContext = new OracleTaskContext(connectorConfig.getConfig(), connectorConfig);
        this.databaseSchema =
                OracleUtils.createOracleDatabaseSchema(
                        connectorConfig, connection, this.taskContext);
        // todo logMiner or xStream
        this.offsetContext =
                loadStartingOffsetState(
                        new BufferedLogMinerOracleOffsetContextLoader(connectorConfig),
                        sourceSplitBase);
        this.partition =
                new OraclePartition(
                        connectorConfig.getLogicalName(), connectorConfig.getDatabaseName());
        validateAndLoadSchemaHistory(offsetContext, databaseSchema);
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
                        topicNamingStrategy,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        new OracleSchemaChangeEventHandler());

        final CapturedTablesSupplier capturedTablesSupplier = Collections::emptyList;
        final OracleChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new OracleChangeEventSourceMetricsFactory(
                        new LogMinerStreamingChangeEventSourceMetrics(
                                taskContext,
                                queue,
                                metadataProvider,
                                connectorConfig,
                                capturedTablesSupplier));
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                (AbstractOracleStreamingChangeEventSourceMetrics)
                        changeEventSourceMetricsFactory.getStreamingMetrics(
                                taskContext, queue, metadataProvider, capturedTablesSupplier);
        this.errorHandler = new OracleErrorHandler(connectorConfig, queue, null);
    }

    @Override
    public OracleSourceConfig getSourceConfig() {
        return (OracleSourceConfig) sourceConfig;
    }

    public OracleSourceConnection getConnection() {
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

    public AbstractOracleStreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
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
            // Debezium 3.4.2+ no longer injects a ROWID Connect header. When absent,
            // fall back to SCN-range inclusion: the backfill reader already constrains
            // events to the split's SCN window, so accepting all events is safe for
            // single-chunk (null-bounded) splits. For multi-chunk tables without a PK,
            // configure scan.incremental.snapshot.chunk.key-column explicitly.
            if (!headers.iterator().hasNext()) {
                return true;
            }
            ROWID rowId = null;
            try {
                rowId = new ROWID(headers.iterator().next().value().toString());
            } catch (SQLException e) {
                LOG.error("{} can not convert to RowId", record);
            }
            Object[] rowIds = new ROWID[] {rowId};
            return SplitKeyUtils.splitKeyRangeContains(rowIds, splitStart, splitEnd);
        } else {
            // config chunk key column compare
            Object[] key = SplitKeyUtils.getSplitKey(splitKeyType, record, getSchemaNameAdjuster());
            return SplitKeyUtils.splitKeyRangeContains(key, splitStart, splitEnd);
        }
    }

    @Override
    public boolean supportsSplitKeyOptimization() {
        return false;
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

    private void validateAndLoadSchemaHistory(
            OracleOffsetContext offset, OracleDatabaseSchema schema) {
        schema.initializeStorage();
        try {
            schema.recover(Offsets.of(partition, offset));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while recovering schema history", e);
        }
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
