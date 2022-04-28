/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.oracle.source.assigner.splitter.OracleChunkSplitter;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfig;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleScanFetchTask;
import com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import com.ververica.cdc.connectors.oracle.source.reader.fetch.OracleStreamFetchTask;
import com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils;
import com.ververica.cdc.connectors.oracle.source.utils.OracleSchema;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils.createOracleConnection;
import static com.ververica.cdc.connectors.oracle.source.utils.OracleConnectionUtils.currentRedoLogOffset;

/** The {@link JdbcDataSourceDialect} implementation for Oracle datasource. */
@Experimental
public class OracleDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final OracleSourceConfigFactory configFactory;
    private final OracleSourceConfig sourceConfig;
    private transient OracleSchema oracleSchema;

    public OracleDialect(OracleSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "Oracle";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentRedoLogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the redoLog offset error", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            OracleConnection oracleConnection = (OracleConnection) jdbcConnection;
            return oracleConnection.getOracleVersion().getMajor() == 11;
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading oracle variables: " + e.getMessage(), e);
        }
    }

    @Override
    public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
        return OracleConnectionUtils.createOracleConnection(
                sourceConfig.getDbzConnectorConfig().getJdbcConfig());
        //        return JdbcDataSourceDialect.super.openJdbcConnection(sourceConfig);
    }

    @Override
    public ChunkSplitter<TableId> createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new OracleChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new OraclePooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        OracleSourceConfig oracleSourceConfig = (OracleSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return OracleConnectionUtils.listTables(
                    jdbcConnection, oracleSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (OracleConnection jdbc = createOracleConnection(sourceConfig.getDbzConfiguration())) {
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
        if (oracleSchema == null) {
            oracleSchema = new OracleSchema(sourceConfig);
        }
        return oracleSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public OracleSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        final OracleConnection jdbcConnection =
                createOracleConnection(taskSourceConfig.getDbzConfiguration());
        return new OracleSourceFetchTaskContext(taskSourceConfig, this, jdbcConnection);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new OracleScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new OracleStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }
}
