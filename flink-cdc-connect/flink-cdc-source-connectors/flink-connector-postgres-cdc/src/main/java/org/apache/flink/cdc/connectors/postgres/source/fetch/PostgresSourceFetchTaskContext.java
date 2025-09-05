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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.ChunkUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.DROP_SLOT_ON_STOP;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;

/** The context of {@link PostgresScanFetchTask} and {@link PostgresStreamFetchTask}. */
public class PostgresSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final String CONNECTION_NAME = "postgres-fetch-task-connection";

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceFetchTaskContext.class);

    private PostgresTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private PostgresConnection jdbcConnection;
    private ReplicationConnection replicationConnection;
    private PostgresOffsetContext offsetContext;
    private PostgresPartition partition;
    private PostgresSchema schema;
    private ErrorHandler errorHandler;
    private CDCPostgresDispatcher postgresDispatcher;
    private EventMetadataProvider metadataProvider;
    private SnapshotChangeEventSourceMetrics<PostgresPartition> snapshotChangeEventSourceMetrics;
    private Snapshotter snapShotter;

    public PostgresSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, PostgresDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return (PostgresConnectorConfig) super.getDbzConnectorConfig();
    }

    private PostgresOffsetContext loadStartingOffsetState(
            PostgresOffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? new PostgresOffsetFactory()
                                .createInitialOffset() // get an offset for starting snapshot
                        : sourceSplitBase.asStreamSplit().getStartingOffset();

        return PostgresOffsetUtils.getPostgresOffsetContext(loader, offset);
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        LOG.debug("Configuring PostgresSourceFetchTaskContext for split: {}", sourceSplitBase);
        PostgresConnectorConfig dbzConfig = getDbzConnectorConfig();
        if (sourceSplitBase instanceof SnapshotSplit) {
            dbzConfig =
                    new PostgresConnectorConfig(
                            dbzConfig
                                    .getConfig()
                                    .edit()
                                    .with(
                                            "table.include.list",
                                            ((SnapshotSplit) sourceSplitBase)
                                                    .getTableId()
                                                    .toString())
                                    .with(
                                            SLOT_NAME.name(),
                                            ((PostgresSourceConfig) sourceConfig)
                                                    .getSlotNameForBackfillTask())
                                    // drop slot for backfill stream split
                                    .with(DROP_SLOT_ON_STOP.name(), true)
                                    // Disable heartbeat event in snapshot split fetcher
                                    .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                                    .build());
        } else {
            dbzConfig =
                    new PostgresConnectorConfig(
                            dbzConfig
                                    .getConfig()
                                    .edit()
                                    // never drop slot for stream split, which is also global split
                                    .with(DROP_SLOT_ON_STOP.name(), false)
                                    .build());
        }

        LOG.info("PostgresConnectorConfig is ", dbzConfig.getConfig().asProperties().toString());
        setDbzConnectorConfig(dbzConfig);
        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        dbzConfig.getConfig().getString(SNAPSHOT_MODE));
        this.snapShotter = snapshotMode.getSnapshotter(dbzConfig.getConfig());

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        this.jdbcConnection =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(), valueConverterBuilder, CONNECTION_NAME);

        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(dbzConfig);
        EmbeddedFlinkDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                sourceSplitBase.getTableSchemas().values());

        try {
            this.schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            dbzConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresSchema", e);
        }

        this.offsetContext =
                loadStartingOffsetState(
                        new PostgresOffsetContext.Loader(dbzConfig), sourceSplitBase);
        this.partition = new PostgresPartition(dbzConfig.getLogicalName());
        this.taskContext = PostgresObjectUtils.newTaskContext(dbzConfig, schema, topicSelector);

        if (replicationConnection == null) {
            replicationConnection =
                    createReplicationConnection(
                            this.taskContext,
                            jdbcConnection,
                            this.snapShotter.shouldSnapshot(),
                            dbzConfig);
        }

        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(dbzConfig.getPollInterval())
                        .maxBatchSize(dbzConfig.getMaxBatchSize())
                        .maxQueueSize(dbzConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(dbzConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "postgres-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();

        this.errorHandler = new PostgresErrorHandler(getDbzConnectorConfig(), queue);
        this.metadataProvider = PostgresObjectUtils.newEventMetadataProvider();

        PostgresConnectorConfig finalDbzConfig = dbzConfig;
        this.postgresDispatcher =
                new CDCPostgresDispatcher(
                        finalDbzConfig,
                        topicSelector,
                        schema,
                        queue,
                        finalDbzConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        new HeartbeatFactory<>(
                                dbzConfig,
                                topicSelector,
                                schemaNameAdjuster,
                                () ->
                                        new PostgresConnection(
                                                finalDbzConfig.getJdbcConfig(),
                                                PostgresConnection.CONNECTION_GENERAL),
                                exception -> {
                                    String sqlErrorId = exception.getSQLState();
                                    switch (sqlErrorId) {
                                        case "57P01":
                                            // Postgres error admin_shutdown, see
                                            // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                            throw new DebeziumException(
                                                    "Could not execute heartbeat action query (Error: "
                                                            + sqlErrorId
                                                            + ")",
                                                    exception);
                                        case "57P03":
                                            // Postgres error cannot_connect_now, see
                                            // https://www.postgresql.org/docs/12/errcodes-appendix.html
                                            throw new RetriableException(
                                                    "Could not execute heartbeat action query (Error: "
                                                            + sqlErrorId
                                                            + ")",
                                                    exception);
                                        default:
                                            break;
                                    }
                                }),
                        schemaNameAdjuster);

        ChangeEventSourceMetricsFactory<PostgresPartition> metricsFactory =
                new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                metricsFactory.getSnapshotMetrics(taskContext, queue, metadataProvider);
    }

    @Override
    public PostgresSchema getDatabaseSchema() {
        return schema;
    }

    @Override
    public RowType getSplitType(Table table) {
        Column splitColumn = ChunkUtils.getSplitColumn(table, sourceConfig.getChunkKeyColumn());
        return ChunkUtils.getSplitType(splitColumn);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public PostgresEventDispatcher<TableId> getEventDispatcher() {
        return postgresDispatcher;
    }

    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        return postgresDispatcher;
    }

    @Override
    public PostgresOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public PostgresPartition getPartition() {
        return partition;
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
    public TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return PostgresOffset.of(sourceRecord);
    }

    @Override
    public void close() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (replicationConnection != null) {
            replicationConnection.close();
        }
    }

    public PostgresConnection getConnection() {
        return jdbcConnection;
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }

    public ReplicationConnection getReplicationConnection() {
        return replicationConnection;
    }

    public SnapshotChangeEventSourceMetrics<PostgresPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public Snapshotter getSnapShotter() {
        return snapShotter;
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        return PostgresConnectorConfig.LogicalDecoder.parse(
                        sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name()))
                .getPostgresPluginName();
    }
}
