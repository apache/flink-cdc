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

package org.apache.flink.cdc.connectors.db2.source.dialect;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask.Context;
import org.apache.flink.cdc.connectors.db2.source.config.Db2SourceConfig;
import org.apache.flink.cdc.connectors.db2.source.fetch.Db2ScanFetchTask;
import org.apache.flink.cdc.connectors.db2.source.fetch.Db2SourceFetchTaskContext;
import org.apache.flink.cdc.connectors.db2.source.fetch.Db2StreamFetchTask;
import org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.db2.Db2Connection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.cdc.connectors.db2.source.utils.Db2ConnectionUtils.createDb2Connection;
import static org.apache.flink.cdc.connectors.db2.source.utils.Db2Utils.currentLsn;

/** The {@link JdbcDataSourceDialect} implementation for Db2 datasource. */
@Experimental
public class Db2Dialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final Db2SourceConfig sourceConfig;
    private transient Db2Schema db2Schema;
    private transient Tables.TableFilter filters;

    public Db2Dialect(Db2SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public String getName() {
        return "Db2";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentLsn((Db2Connection) jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the redoLog offset error", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        return true;
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return createDb2Connection(sourceConfig.getDbzConnectorConfig());
    }

    @Override
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new Db2ChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new Db2PooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        Db2SourceConfig db2SourceConfig = (Db2SourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return Db2ConnectionUtils.listTables(
                    jdbcConnection, db2SourceConfig.getDbzConnectorConfig().getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (Db2Connection jdbc = createDb2Connection(sourceConfig.getDbzConnectorConfig())) {
            // fetch table schemas
            Map<TableId, TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChange tableSchema = queryTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error to discover table schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
        if (db2Schema == null) {
            db2Schema = new Db2Schema();
        }
        return db2Schema.getTableSchema(
                jdbc,
                tableId,
                sourceConfig.getDbzConnectorConfig().getTableFilters().dataCollectionFilter());
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new Db2ScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new Db2StreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }

    /**
     * The task context used for fetch task to fetch data from external systems.
     *
     * @param sourceConfig
     */
    @Override
    public Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
        final Db2Connection jdbcConnection =
                createDb2Connection(sourceConfig.getDbzConnectorConfig());
        final Db2Connection metaDataConnection =
                createDb2Connection(sourceConfig.getDbzConnectorConfig());
        return new Db2SourceFetchTaskContext(
                sourceConfig, this, jdbcConnection, metaDataConnection);
    }

    /**
     * Check if the tableId is included in SourceConfig.
     *
     * @param sourceConfig
     * @param tableId
     */
    @Override
    public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
        if (filters == null) {
            this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
        }

        return filters.isIncluded(tableId);
    }
}
