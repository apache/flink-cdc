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

package org.apache.flink.cdc.connectors.db2.source.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.dialect.Db2Dialect;
import org.apache.flink.cdc.connectors.db2.source.handler.Db2SchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.db2.source.offset.LsnOffset;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2Utils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.ChangeEventQueue.Builder;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2Connector;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.connector.db2.Db2OffsetContext.Loader;
import io.debezium.connector.db2.Db2Partition;
import io.debezium.connector.db2.Db2TaskContext;
import io.debezium.connector.db2.Db2TopicSelector;
import io.debezium.connector.db2.SourceInfo;
import io.debezium.data.Envelope.FieldName;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.Map;

/** The context for fetch task that fetching data of snapshot split from Db2 data source. */
public class Db2SourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    /** Connection used for reading CDC tables. */
    private final Db2Connection connection;

    private final Db2Connection metaDataConnection;

    private final Db2EventMetadataProvider metadataProvider;
    private Db2OffsetContext offsetContext;
    private Db2Partition partition;
    private Db2DatabaseSchema databaseSchema;
    private JdbcSourceEventDispatcher<Db2Partition> dispatcher;
    private ErrorHandler errorHandler;
    private ChangeEventQueue<DataChangeEvent> queue;
    private Db2TaskContext taskContext;
    private TopicSelector<TableId> topicSelector;
    private EventDispatcher.SnapshotReceiver<Db2Partition> snapshotReceiver;
    private SnapshotChangeEventSourceMetrics<Db2Partition> snapshotChangeEventSourceMetrics;
    private StreamingChangeEventSourceMetrics<Db2Partition> streamingChangeEventSourceMetrics;

    public Db2SourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            Db2Dialect dataSourceDialect,
            Db2Connection connection,
            Db2Connection metaDataConnection) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.metadataProvider = new Db2EventMetadataProvider();
        this.metaDataConnection = metaDataConnection;
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // initial stateful objects
        final Db2ConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.topicSelector = Db2TopicSelector.defaultSelector(connectorConfig);
        EmbeddedFlinkDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                sourceSplitBase.getTableSchemas().values());

        this.databaseSchema = Db2Utils.createDb2DatabaseSchema(connectorConfig, connection);
        this.offsetContext = loadStartingOffsetState(new Loader(connectorConfig), sourceSplitBase);

        String serverName = connectorConfig.getLogicalName();
        this.partition = new Db2Partition(serverName);

        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new Db2TaskContext(connectorConfig, databaseSchema);

        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? getSourceConfig().getSplitSize()
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () -> taskContext.configureLoggingContext("Db2-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        //                         .buffering()
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
                        new Db2SchemaChangeEventHandler());

        this.snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();

        final DefaultChangeEventSourceMetricsFactory<Db2Partition> changeEventSourceMetricsFactory =
                new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getStreamingMetrics(
                        taskContext, queue, metadataProvider);
        this.errorHandler = new ErrorHandler(Db2Connector.class, connectorConfig, queue);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private Db2OffsetContext loadStartingOffsetState(
            Db2OffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : sourceSplitBase.asStreamSplit().getStartingOffset();

        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(Db2OffsetContext offset, Db2DatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(partition, offset);
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return Db2Utils.getLsn(record);
    }

    @Override
    public Db2DatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        Column splitColumn = Db2Utils.getSplitColumn(table, sourceConfig.getChunkKeyColumn());
        return Db2Utils.getSplitType(splitColumn);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public Db2ConnectorConfig getDbzConnectorConfig() {
        return (Db2ConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public Db2SourceConfig getSourceConfig() {
        return (Db2SourceConfig) sourceConfig;
    }

    @Override
    public JdbcSourceEventDispatcher<Db2Partition> getDispatcher() {
        return dispatcher;
    }

    public EventDispatcher.SnapshotReceiver<Db2Partition> getSnapshotReceiver() {
        return snapshotReceiver;
    }

    @Override
    public Db2OffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public Db2Partition getPartition() {
        return partition;
    }

    public Db2Connection getConnection() {
        return connection;
    }

    public Db2Connection getMetaDataConnection() {
        return metaDataConnection;
    }

    public SnapshotChangeEventSourceMetrics<Db2Partition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public StreamingChangeEventSourceMetrics<Db2Partition> getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public void close() throws Exception {
        metaDataConnection.commit();
        connection.commit();
        metaDataConnection.close();
        connection.close();
    }

    @Override
    public SchemaNameAdjuster getSchemaNameAdjuster() {
        return schemaNameAdjuster;
    }

    /** Copied from debezium for accessing here. */
    public static class Db2EventMetadataProvider implements EventMetadataProvider {

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(FieldName.SOURCE);
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
            final Struct sourceInfo = value.getStruct(FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY),
                    SourceInfo.CHANGE_LSN_KEY, sourceInfo.getString(SourceInfo.CHANGE_LSN_KEY));
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY);
        }
    }
}
