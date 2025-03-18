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

package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.tidb.TiDBEventMetadataProvider;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.connector.tidb.TiDBTaskContext;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.handler.TiDBErrorHandler;
import org.apache.flink.cdc.connectors.tidb.source.handler.TiDBSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetFactory;
import org.apache.flink.cdc.connectors.tidb.source.offset.EventOffsetUtils;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceFetchTaskContext.class);

    private TiDBTaskContext tidbTaskContext;

    private final TiDBConnection connection;
    private TiDBDatabaseSchema tiDBDatabaseSchema;
    private EventOffsetContext offsetContext;
    private SnapshotChangeEventSourceMetrics<TiDBPartition> snapshotChangeEventSourceMetrics;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<TiDBPartition> dispatcher;
    private TiDBPartition tiDBPartition;
    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;
    private EventMetadataProvider metadataProvider;

    public TiDBSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            TiDBConnection connection) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.metadataProvider = new TiDBEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        final TiDBConnectorConfig connectorConfig = getDbzConnectorConfig();
        final boolean tableIdCaseInsensitive =
                dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
        TopicSelector<TableId> topicSelector =
                TopicSelector.defaultSelector(
                        connectorConfig,
                        (tableId, prefix, delimiter) ->
                                String.join(delimiter, prefix, tableId.identifier()));
        try {
            this.tiDBDatabaseSchema =
                    TiDBUtils.newSchema(
                            connection, connectorConfig, topicSelector, tableIdCaseInsensitive);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize TiDBSchema", e);
        }

        this.tiDBPartition = new TiDBPartition(connectorConfig.getLogicalName());
        this.tidbTaskContext = new TiDBTaskContext(connectorConfig, tiDBDatabaseSchema);
        this.offsetContext =
                loadStartingOffsetState(
                        new EventOffsetContext.Loader(connectorConfig), sourceSplitBase);
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(connectorConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        tidbTaskContext.configureLoggingContext(
                                                "tidb-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.errorHandler =
                new TiDBErrorHandler(
                        (TiDBConnectorConfig) sourceConfig.getDbzConnectorConfig(), queue);
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        tiDBDatabaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        new TiDBSchemaChangeEventHandler());

        ChangeEventSourceMetricsFactory<TiDBPartition> metricsFactory =
                new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                metricsFactory.getSnapshotMetrics(tidbTaskContext, queue, metadataProvider);
    }

    public TiDBConnection getConnection() {
        return connection;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return this.sourceConfig.getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return new EventOffset(record.sourceOffset());
    }

    @Override
    public void close() throws Exception {
        this.connection.close();
    }

    @Override
    public TiDBDatabaseSchema getDatabaseSchema() {
        return tiDBDatabaseSchema;
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        if (this.offsetContext.isSnapshotRunning()) {
            RowType splitKeyType =
                    getSplitType(getDatabaseSchema().tableFor(this.getTableId(record)));
            Object[] key =
                    SourceRecordUtils.getSplitKey(splitKeyType, record, getSchemaNameAdjuster());
            return SourceRecordUtils.splitKeyRangeContains(key, splitStart, splitEnd);
        } else {
            EventOffset newOffset = new EventOffset(record.sourceOffset());
            return SourceRecordUtils.splitKeyRangeContains(
                    new EventOffset[]{newOffset}, splitStart, splitEnd);
        }
    }

    @Override
    public RowType getSplitType(Table table) {
        return TiDBUtils.getSplitType(table);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public JdbcSourceEventDispatcher getEventDispatcher() {
        return dispatcher;
    }

    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        return dispatcher;
    }

    @Override
    public EventOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public TiDBPartition getPartition() {
        return tiDBPartition;
    }

    @Override
    public TiDBConnectorConfig getDbzConnectorConfig() {
        return (TiDBConnectorConfig) super.getDbzConnectorConfig();
    }

    public SnapshotChangeEventSourceMetrics<TiDBPartition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    private EventOffsetContext loadStartingOffsetState(
            EventOffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? new EventOffsetFactory()
                        .createInitialOffset() // get an offset for starting snapshot
                        : sourceSplitBase.asStreamSplit().getStartingOffset();

        return EventOffsetUtils.getEventOffsetContext(loader, offset);
    }

    public TiDBSourceFetchTaskContext getTaskContext() {
        return this;
    }
}
