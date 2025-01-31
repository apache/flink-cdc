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

package org.apache.flink.cdc.connectors.oceanbase.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseSourceConfig;
import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnection;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.LogMessageOffset;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBaseOffsetContext;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBasePartition;
import org.apache.flink.cdc.connectors.oceanbase.source.offset.OceanBaseSourceInfo;
import org.apache.flink.cdc.connectors.oceanbase.source.schema.OceanBaseDatabaseSchema;
import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.Map;

/** The context for fetch task that fetching data of snapshot split from OceanBase data source. */
public class OceanBaseSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private final OceanBaseConnection connection;
    private final OceanBaseEventMetadataProvider metadataProvider;

    private OceanBaseDatabaseSchema databaseSchema;
    private OceanBaseTaskContext taskContext;
    private OceanBaseOffsetContext offsetContext;
    private OceanBasePartition partition;
    private JdbcSourceEventDispatcher<OceanBasePartition> dispatcher;

    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;

    public OceanBaseSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            OceanBaseConnection connection) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.metadataProvider = new OceanBaseEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        final OceanBaseConnectorConfig connectorConfig = getDbzConnectorConfig();
        final boolean tableIdCaseInsensitive =
                dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);

        TopicSelector<TableId> topicSelector =
                TopicSelector.defaultSelector(
                        connectorConfig,
                        (tableId, prefix, delimiter) ->
                                String.join(delimiter, prefix, tableId.identifier()));

        this.databaseSchema =
                new OceanBaseDatabaseSchema(connectorConfig, topicSelector, tableIdCaseInsensitive);
        this.offsetContext =
                loadStartingOffsetState(
                        new OceanBaseOffsetContext.Loader(connectorConfig), sourceSplitBase);

        this.partition = new OceanBasePartition(connectorConfig.getLogicalName());

        this.taskContext = new OceanBaseTaskContext(connectorConfig, databaseSchema);
        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? Integer.MAX_VALUE
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
                                                "oceanbase-cdc-connector-task"))
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
                        null);
        this.errorHandler = new ErrorHandler(null, connectorConfig, queue);
    }

    @Override
    public OceanBaseSourceConfig getSourceConfig() {
        return (OceanBaseSourceConfig) sourceConfig;
    }

    public OceanBaseConnection getConnection() {
        return connection;
    }

    @Override
    public OceanBasePartition getPartition() {
        return partition;
    }

    public OceanBaseTaskContext getTaskContext() {
        return taskContext;
    }

    @Override
    public OceanBaseConnectorConfig getDbzConnectorConfig() {
        return (OceanBaseConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public OceanBaseOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public OceanBaseDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        return OceanBaseUtils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher<OceanBasePartition> getEventDispatcher() {
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

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return new LogMessageOffset(sourceRecord.sourceOffset());
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private OceanBaseOffsetContext loadStartingOffsetState(
            OffsetContext.Loader<OceanBaseOffsetContext> loader, SourceSplitBase sourceSplit) {
        Offset offset =
                sourceSplit.isSnapshotSplit()
                        ? LogMessageOffset.INITIAL_OFFSET
                        : sourceSplit.asStreamSplit().getStartingOffset();
        return loader.load(offset.getOffset());
    }

    /** The {@link EventMetadataProvider} implementation for OceanBase. */
    public static class OceanBaseEventMetadataProvider implements EventMetadataProvider {

        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceInfo == null) {
                return null;
            }
            final Long ts = sourceInfo.getInt64(OceanBaseSourceInfo.TIMESTAMP_KEY);
            return ts == null ? null : Instant.ofEpochMilli(ts);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceInfo == null) {
                return null;
            }
            return Collect.hashMapOf(
                    "tenant",
                    sourceInfo.getString(OceanBaseSourceInfo.TENANT_KEY),
                    "timestamp",
                    sourceInfo.getString(OceanBaseSourceInfo.TIMESTAMP_KEY),
                    "transaction_id",
                    sourceInfo.getString(OceanBaseSourceInfo.TRANSACTION_ID_KEY));
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceInfo == null) {
                return null;
            }
            return sourceInfo.getString(OceanBaseSourceInfo.TRANSACTION_ID_KEY);
        }
    }
}
