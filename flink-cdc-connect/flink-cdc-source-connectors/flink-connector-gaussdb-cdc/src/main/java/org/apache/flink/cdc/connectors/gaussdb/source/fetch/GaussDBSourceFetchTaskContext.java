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

package org.apache.flink.cdc.connectors.gaussdb.source.fetch;

import org.apache.flink.cdc.connectors.base.WatermarkDispatcher;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.GaussDBDialect;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.CustomGaussDBSchema;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.GaussDBTypeUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.gaussdb.GaussDBConnectorConfig;
import io.debezium.connector.gaussdb.connection.GaussDBConnection;
import io.debezium.connector.gaussdb.decoder.MppdbDecodingMessageDecoder;
import io.debezium.connector.postgresql.connection.AbstractMessageDecoder;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.LoggingContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;

/**
 * The context of {@link GaussDBScanFetchTask} and
 * {@link GaussDBStreamFetchTask}.
 */
public class GaussDBSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSourceFetchTaskContext.class);

    private GaussDBConnection jdbcConnection;
    private CustomGaussDBSchema schema;
    private AbstractMessageDecoder messageDecoder;
    private Partition partition;
    private ChangeEventQueue<DataChangeEvent> queue;
    private io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection replicationConnection;
    // Separate replication connection for backfill tasks to avoid slot contention
    private io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection backfillReplicationConnection;

    public GaussDBSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, GaussDBDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
    }

    @Override
    public GaussDBConnectorConfig getDbzConnectorConfig() {
        return (GaussDBConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {

        GaussDBConnectorConfig dbzConfig = getDbzConnectorConfig();

        setDbzConnectorConfig(dbzConfig);

        // Initialize ChangeEventQueue for passing events between fetcher and source
        // reader
        if (queue == null) {
            final int queueSize = sourceSplitBase instanceof SnapshotSplit
                    ? sourceConfig.getSplitSize()
                    : dbzConfig.getMaxQueueSize();
            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(dbzConfig.getPollInterval())
                    .maxBatchSize(dbzConfig.getMaxBatchSize())
                    .maxQueueSize(queueSize)
                    .maxQueueSizeInBytes(dbzConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(
                            () -> LoggingContext.forConnector(
                                    "gaussdb-cdc",
                                    dbzConfig.getLogicalName(),
                                    "gaussdb-cdc-connector-task"))
                    // do not buffer any element, we use signal event
                    // .buffering()
                    .build();
        }

        // Initialize partition
        if (partition == null) {
            partition = new GaussDBPartition(dbzConfig.getLogicalName());
        }

        // Initialize JDBC connection
        try {
            if (jdbcConnection == null) {
                jdbcConnection = new GaussDBConnection(
                        dbzConfig.getJdbcConfig(), "gaussdb-fetch-task-connection");
                jdbcConnection.connect();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create GaussDB JDBC connection", e);
        }

        // Initialize schema cache
        if (schema == null) {
            schema = new CustomGaussDBSchema(jdbcConnection);
        }

        // Register table schemas for this split
        if (sourceSplitBase.getTableSchemas() != null) {
            sourceSplitBase
                    .getTableSchemas()
                    .values()
                    .forEach(tableChange -> schema.applyTableChange(tableChange));
        }

        // Initialize message decoder for streaming
        if (messageDecoder == null) {
            messageDecoder = new MppdbDecodingMessageDecoder();
        }

        // Initialize replication connection for streaming (unbounded stream splits
        // only)
        // For bounded splits (backfill), use the separate backfill connection
        if (replicationConnection == null && !isBoundedRead(sourceSplitBase)) {
            try {
                replicationConnection = createReplicationConnection(dbzConfig, false);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create GaussDB replication connection", e);
            }
        }

        // Initialize backfill replication connection for bounded reads
        // This includes: SnapshotSplits and StreamSplits with ending offset (backfill)
        // Uses a separate slot to avoid contention with the main streaming slot
        if (backfillReplicationConnection == null && isBoundedRead(sourceSplitBase)) {
            try {
                LOG.info("Creating backfill replication connection for bounded read");
                backfillReplicationConnection = createReplicationConnection(dbzConfig, true);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create GaussDB backfill replication connection", e);
            }
        }
    }

    /**
     * Checks if the split represents a bounded read that requires a separate
     * backfill connection.
     * Bounded reads include SnapshotSplits and StreamSplits with a valid ending
     * offset.
     */
    private boolean isBoundedRead(SourceSplitBase split) {
        if (split instanceof SnapshotSplit) {
            return true;
        }
        if (split instanceof StreamSplit) {
            StreamSplit streamSplit = (StreamSplit) split;
            org.apache.flink.cdc.connectors.base.source.meta.offset.Offset endingOffset = streamSplit.getEndingOffset();
            if (endingOffset != null
                    && endingOffset instanceof org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) {
                org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset gaussDBEndOffset = (org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset) endingOffset;
                return gaussDBEndOffset.getLsn() != null && gaussDBEndOffset.getLsn().isValid();
            }
        }
        return false;
    }

    private io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection createReplicationConnection(
            GaussDBConnectorConfig config, boolean forBackfill) throws Exception {
        GaussDBSourceConfig gaussDBConfig = (GaussDBSourceConfig) sourceConfig;
        // Use separate slot for backfill to avoid contention
        String slotName = forBackfill
                ? gaussDBConfig.getSlotNameForBackfillTask()
                : gaussDBConfig.getSlotName();
        String pluginName = gaussDBConfig.getDecodingPluginName();
        // Drop slot on close for backfill tasks (temporary slot)
        boolean dropSlotOnClose = forBackfill;
        java.time.Duration statusUpdateInterval = java.time.Duration.ofSeconds(10);

        LOG.info("Creating replication connection with slot: {}, forBackfill: {}, dropSlotOnClose: {}",
                slotName, forBackfill, dropSlotOnClose);

        // Create PostgresConnection for TypeRegistry using PostgreSQL JDBC (GaussDB
        // compatible)
        io.debezium.connector.postgresql.connection.PostgresConnection pgConnection = createPostgresConnection(config);

        // Create TypeRegistry with valid PostgresConnection
        io.debezium.connector.postgresql.TypeRegistry typeRegistry = new io.debezium.connector.postgresql.TypeRegistry(
                pgConnection);

        return new io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection(
                config,
                slotName,
                pluginName,
                dropSlotOnClose,
                statusUpdateInterval,
                (MppdbDecodingMessageDecoder) messageDecoder,
                typeRegistry,
                null);
    }

    /**
     * Creates a PostgresConnection using PostgreSQL JDBC driver for TypeRegistry
     * initialization.
     * GaussDB is compatible with PostgreSQL protocol, so we can use PostgreSQL JDBC
     * driver.
     */
    private io.debezium.connector.postgresql.connection.PostgresConnection createPostgresConnection(
            GaussDBConnectorConfig config) {
        // Build PostgreSQL-compatible JDBC configuration
        io.debezium.jdbc.JdbcConfiguration jdbcConfig = io.debezium.jdbc.JdbcConfiguration.create()
                .with(
                        io.debezium.jdbc.JdbcConfiguration.HOSTNAME,
                        config.getJdbcConfig().getHostname())
                .with(
                        io.debezium.jdbc.JdbcConfiguration.PORT,
                        config.getJdbcConfig().getPort())
                .with(
                        io.debezium.jdbc.JdbcConfiguration.USER,
                        config.getJdbcConfig().getUser())
                .with(
                        io.debezium.jdbc.JdbcConfiguration.PASSWORD,
                        config.getJdbcConfig().getPassword())
                .with(
                        io.debezium.jdbc.JdbcConfiguration.DATABASE,
                        config.getJdbcConfig().getDatabase())
                .build();

        // Create a PostgresConnection for TypeRegistry initialization.
        // Note: Debezium validates the server version (>= 9.4) during connection
        // initialization.
        // GaussDB may report a non-PostgreSQL version format (e.g. "gaussdb (GaussDB
        // Kernel
        // 505...)"),
        // which makes the PostgreSQL JDBC driver report an incompatible major/minor
        // version.
        // Use a GaussDB-specific PostgresConnection wrapper to skip that validation.
        return new io.debezium.connector.gaussdb.connection.GaussDBPostgresConnection(
                jdbcConfig, "gaussdb-type-registry");
    }

    @Override
    public RelationalDatabaseSchema getDatabaseSchema() {
        // Return null for now as CustomGaussDBSchema is not a RelationalDatabaseSchema
        // Full schema integration will be implemented in Sprint 3
        return null;
    }

    public CustomGaussDBSchema getCustomSchema() {
        return schema;
    }

    @Override
    public RowType getSplitType(Table table) {
        Column splitColumn = getSplitColumn(table);
        return getSplitType(splitColumn);
    }

    public GaussDBConnection getConnection() {
        return jdbcConnection;
    }

    @Override
    public Partition getPartition() {
        return partition;
    }

    @Override
    public OffsetContext getOffsetContext() {
        // Offset context will be implemented in Sprint 3
        // For now, return null as offset management is not yet fully integrated
        return null;
    }

    @Override
    public WatermarkDispatcher getWaterMarkDispatcher() {
        // Watermark dispatcher will be implemented in Sprint 3
        // For now, return null as watermark management is not yet fully integrated
        return null;
    }

    @Override
    public JdbcSourceEventDispatcher getEventDispatcher() {
        // GaussDB uses a simplified event dispatcher for v1.0
        // Full event dispatcher will be implemented in future versions
        return null;
    }

    @Override
    public io.debezium.pipeline.ErrorHandler getErrorHandler() {
        // Error handler will be implemented in Sprint 3
        // For now, return null as error handling is not yet fully integrated
        return null;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        org.apache.kafka.connect.data.Struct value = (org.apache.kafka.connect.data.Struct) record.value();
        org.apache.kafka.connect.data.Struct source = value.getStruct(io.debezium.data.Envelope.FieldName.SOURCE);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

    @Override
    public org.apache.flink.cdc.connectors.base.source.meta.offset.Offset getStreamOffset(
            SourceRecord sourceRecord) {
        // Stream offset extraction will be implemented in Sprint 3
        // For now, return null as offset management is not yet fully integrated
        return null;
    }

    @Override
    public io.debezium.relational.Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public void close() throws Exception {
        if (replicationConnection != null) {
            replicationConnection.close();
        }
        if (backfillReplicationConnection != null) {
            backfillReplicationConnection.close();
        }
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (messageDecoder != null) {
            messageDecoder.close();
        }
    }

    public AbstractMessageDecoder getMessageDecoder() {
        return messageDecoder;
    }

    public io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection getReplicationConnection() {
        return replicationConnection;
    }

    /**
     * Gets the backfill replication connection that uses a separate slot.
     * Should be used for snapshot/backfill tasks to avoid contention with the main
     * streaming slot.
     */
    public io.debezium.connector.gaussdb.connection.GaussDBReplicationConnection getBackfillReplicationConnection() {
        return backfillReplicationConnection;
    }

    public String getSlotName() {
        return ((GaussDBSourceConfig) sourceConfig).getSlotName();
    }

    public String getDecodingPluginName() {
        return ((GaussDBSourceConfig) sourceConfig).getDecodingPluginName();
    }

    /**
     * Gets the split column for the given table. Uses the configured chunk key
     * column if available,
     * otherwise uses the first primary key column.
     */
    private Column getSplitColumn(Table table) {
        String chunkKeyColumn = sourceConfig.getChunkKeyColumn();
        if (chunkKeyColumn != null) {
            Column column = table.columnWithName(chunkKeyColumn);
            if (column != null) {
                return column;
            }
            LOG.warn(
                    "Configured chunk key column '{}' not found in table , using first primary key column",
                    chunkKeyColumn,
                    table.id());
        }

        List<String> primaryKeys = table.primaryKeyColumnNames();
        if (primaryKeys.isEmpty()) {
            throw new IllegalStateException(
                    "Table " + table.id() + " has no primary key, cannot determine split column");
        }

        return table.columnWithName(primaryKeys.get(0));
    }

    /** Converts a Debezium Column to a Flink RowType for split key handling. */
    private RowType getSplitType(Column splitColumn) {
        String typeName = splitColumn.typeName();
        org.apache.flink.table.types.DataType flinkType = GaussDBTypeUtils.convertGaussDBType(typeName);

        return RowType.of(flinkType.getLogicalType());
    }

    /** Simple Partition implementation for GaussDB. */
    private static class GaussDBPartition implements Partition {
        private final String serverName;

        public GaussDBPartition(String serverName) {
            this.serverName = serverName;
        }

        @Override
        public java.util.Map<String, String> getSourcePartition() {
            return java.util.Collections.singletonMap("server", serverName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            GaussDBPartition that = (GaussDBPartition) obj;
            return java.util.Objects.equals(serverName, that.serverName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(serverName);
        }
    }
}
