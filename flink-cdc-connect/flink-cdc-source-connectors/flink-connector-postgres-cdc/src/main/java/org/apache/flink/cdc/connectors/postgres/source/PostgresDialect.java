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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresScanFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresStreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnectionUtils;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.TopicSelector;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresObjectUtils.createReplicationConnection;
import static io.debezium.connector.postgresql.PostgresObjectUtils.newPostgresValueConverterBuilder;
import static io.debezium.connector.postgresql.Utils.currentOffset;

/** The dialect for Postgres. */
public class PostgresDialect implements JdbcDataSourceDialect {
    private static final long serialVersionUID = 1L;
    private static final String CONNECTION_NAME = "postgres-cdc-connector";

    private final PostgresSourceConfig sourceConfig;
    private transient Tables.TableFilter filters;
    private transient CustomPostgresSchema schema;
    @Nullable private PostgresStreamFetchTask streamFetchTask;

    public PostgresDialect(PostgresSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        PostgresSourceConfig postgresSourceConfig = (PostgresSourceConfig) sourceConfig;
        PostgresConnectorConfig dbzConfig = postgresSourceConfig.getDbzConnectorConfig();

        PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                newPostgresValueConverterBuilder(dbzConfig);
        PostgresConnection jdbc =
                new PostgresConnection(
                        dbzConfig.getJdbcConfig(),
                        valueConverterBuilder,
                        CONNECTION_NAME,
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));

        try {
            jdbc.connect();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    public PostgresConnection openJdbcConnection() {
        return (PostgresConnection) openJdbcConnection(sourceConfig);
    }

    public PostgresReplicationConnection openPostgresReplicationConnection(
            PostgresConnection jdbcConnection) {
        try {
            PostgresConnectorConfig pgConnectorConfig = sourceConfig.getDbzConnectorConfig();
            TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(pgConnectorConfig);
            PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                    newPostgresValueConverterBuilder(pgConnectorConfig);
            PostgresSchema schema =
                    PostgresObjectUtils.newSchema(
                            jdbcConnection,
                            pgConnectorConfig,
                            jdbcConnection.getTypeRegistry(),
                            topicSelector,
                            valueConverterBuilder.build(jdbcConnection.getTypeRegistry()));
            PostgresTaskContext taskContext =
                    PostgresObjectUtils.newTaskContext(pgConnectorConfig, schema, topicSelector);
            return (PostgresReplicationConnection)
                    createReplicationConnection(
                            taskContext, jdbcConnection, false, pgConnectorConfig);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize PostgresReplicationConnection", e);
        }
    }

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return currentOffset((PostgresConnection) jdbc);

        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public Offset displayCommittedOffset(JdbcSourceConfig sourceConfig) {

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return PostgresConnectionUtils.committedOffset(
                    (PostgresConnection) jdbc, getSlotName(), getPluginName());

        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // from Postgres docs:
        //
        // SQL is case insensitive about key words and identifiers,
        // except when identifiers are double-quoted to preserve the case
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new PostgresChunkSplitter(
                sourceConfig, this, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new PostgresChunkSplitter(sourceConfig, this, chunkSplitterState);
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    // there is always a single database provided
                    sourceConfig.getDatabaseList().get(0), jdbc, sourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = queryTableSchema(jdbc, capturedTableIds);
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new PostgresConnectionPoolFactory();
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (schema == null) {
            schema = new CustomPostgresSchema((PostgresConnection) jdbc, sourceConfig);
        }
        return schema.getTableSchema(tableId);
    }

    private Map<TableId, TableChange> queryTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds) {
        if (schema == null) {
            schema = new CustomPostgresSchema((PostgresConnection) jdbc, sourceConfig);
        }
        return schema.getTableSchema(tableIds);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new PostgresScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new PostgresStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public JdbcSourceFetchTaskContext createFetchTaskContext(JdbcSourceConfig taskSourceConfig) {
        return new PostgresSourceFetchTaskContext(taskSourceConfig, this);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId, Offset offset) throws Exception {
        if (streamFetchTask != null) {
            streamFetchTask.commitCurrentOffset(offset);
        }
    }

    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }

        return filters.isIncluded(tableId);
    }

    public String getSlotName() {
        return sourceConfig.getDbzProperties().getProperty(SLOT_NAME.name());
    }

    public String getPluginName() {
        return sourceConfig.getDbzProperties().getProperty(PLUGIN_NAME.name());
    }
}
