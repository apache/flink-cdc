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

package org.apache.flink.cdc.connectors.gaussdb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.assigner.state.ChunkSplitterState;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceConfig;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBScanFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.gaussdb.source.fetch.GaussDBStreamFetchTask;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.CustomGaussDBSchema;
import org.apache.flink.cdc.connectors.gaussdb.source.utils.TableDiscoveryUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** The dialect for GaussDB. */
public class GaussDBDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private static final String CONNECTION_NAME = "gaussdb-cdc-connector";

    private final GaussDBSourceConfig sourceConfig;
    private transient Tables.TableFilter filters;
    private transient CustomGaussDBSchema schema;
    private transient GaussDBStreamFetchTask streamFetchTask;

    public GaussDBDialect(GaussDBSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "GaussDB";
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        try {
            GaussDBSourceConfig gaussDBSourceConfig = (GaussDBSourceConfig) sourceConfig;
            JdbcConnection jdbcConnection =
                    new JdbcConnection(
                            gaussDBSourceConfig.getDbzConnectorConfig().getJdbcConfig(),
                            new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
                            "\"",
                            "\"");
            jdbcConnection.connect();
            return jdbcConnection;
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new GaussDBConnectionPoolFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            GaussDBSourceConfig gaussDBSourceConfig = (GaussDBSourceConfig) sourceConfig;
            List<String> schemaList = gaussDBSourceConfig.getSchemaList();

            List<TableId> tables =
                    TableDiscoveryUtils.listTables(
                            // there is always a single database provided
                            sourceConfig.getDatabaseList().get(0),
                            jdbc,
                            sourceConfig.getTableFilters(),
                            schemaList);
            return tables;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            // fetch table schemas
            Map<TableId, TableChange> schemas = queryTableSchema(jdbc, capturedTableIds);
            return schemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (schema == null) {
            schema = new CustomGaussDBSchema(jdbc);
        }
        return schema.getTableSchema(tableId);
    }

    private Map<TableId, TableChange> queryTableSchema(
            JdbcConnection jdbc, List<TableId> tableIds) {
        if (schema == null) {
            schema = new CustomGaussDBSchema(jdbc);
        }
        return schema.getTableSchema(tableIds);
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)) {
            return queryCurrentOffset(jdbc);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to query current offset", e);
        }
    }

    private Offset queryCurrentOffset(JdbcConnection jdbc) throws SQLException {
        // GaussDB is based on PostgreSQL 9.x, use pg_current_xlog_location()
        final String lsnQuery = "SELECT pg_current_xlog_location()";
        return jdbc.queryAndMap(
                lsnQuery,
                rs -> {
                    if (rs.next()) {
                        final String lsnStr = rs.getString(1);
                        return new org.apache.flink.cdc.connectors.gaussdb.source.offset
                                .GaussDBOffset(
                                io.debezium.connector.gaussdb.connection.Lsn.valueOf(lsnStr));
                    }
                    return org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset
                            .INITIAL_OFFSET;
                });
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        // GaussDB follows PostgreSQL behavior: identifiers are case-sensitive when quoted.
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return createChunkSplitter(sourceConfig, ChunkSplitterState.NO_SPLITTING_TABLE_STATE);
    }

    @Override
    public ChunkSplitter createChunkSplitter(
            JdbcSourceConfig sourceConfig, ChunkSplitterState chunkSplitterState) {
        return new GaussDBChunkSplitter(sourceConfig, this, chunkSplitterState);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new GaussDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            this.streamFetchTask = new GaussDBStreamFetchTask(sourceSplitBase.asStreamSplit());
            return this.streamFetchTask;
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
        return new GaussDBSourceFetchTaskContext(sourceConfig, this);
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

    /** Displays a table identifier as {@code schema.table}. */
    public String displayTableId(TableId tableId) {
        if (tableId == null) {
            return "null";
        }
        if (tableId.schema() == null || tableId.schema().isEmpty()) {
            return tableId.table();
        }
        return tableId.schema() + "." + tableId.table();
    }
}
