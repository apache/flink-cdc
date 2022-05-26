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

package com.ververica.cdc.connectors.sqlserver.experimental;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.sqlserver.experimental.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.experimental.config.SqlServerSourceConfigFactory;
import com.ververica.cdc.connectors.sqlserver.experimental.fetch.SqlServerScanFetchTask;
import com.ververica.cdc.connectors.sqlserver.experimental.fetch.SqlServerSourceFetchTaskContext;
import com.ververica.cdc.connectors.sqlserver.experimental.fetch.SqlServerStreamFetchTask;
import com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerSchema;
import com.ververica.cdc.connectors.sqlserver.experimental.utils.TableDiscoveryUtils;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerConnectionUtils.createSqlServerConnection;
import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerConnectionUtils.currentTransactionLogOffset;
import static com.ververica.cdc.connectors.sqlserver.experimental.utils.SqlServerConnectionUtils.isTableIdCaseSensitive;

/** The {@link JdbcDataSourceDialect} implementation for MySQL datasource. */
@Experimental
public class SqlServerDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final SqlServerSourceConfigFactory configFactory;
    private final SqlServerSourceConfig sourceConfig;
    private transient SqlServerSchema sqlServerSchema;

    public SqlServerDialect(SqlServerSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "SqlServer";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentTransactionLogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return isTableIdCaseSensitive(jdbcConnection);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading MySQL variables: " + e.getMessage(), e);
        }
    }

    @Override
    public ChunkSplitter<TableId> createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new SqlServerChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new SqlServerPooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        SqlServerSourceConfig sqlServerSourceConfig = (SqlServerSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, sqlServerSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (SqlServerConnection jdbc =
                createSqlServerConnection(sourceConfig.getDbzConfiguration())) {
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
        if (sqlServerSchema == null) {
            sqlServerSchema =
                    new SqlServerSchema(
                            sourceConfig, isDataCollectionIdCaseSensitive(sourceConfig));
        }
        return sqlServerSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public SqlServerSourceFetchTaskContext createFetchTaskContext(SourceSplitBase sourceSplitBase) {
        final SqlServerConnection dataConnection =
                createSqlServerConnection(sourceConfig.getDbzConfiguration());
        final SqlServerConnection metadataConnection =
                createSqlServerConnection(sourceConfig.getDbzConfiguration());
        return new SqlServerSourceFetchTaskContext(
                sourceConfig, this, dataConnection, metadataConnection);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new SqlServerScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new SqlServerStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }
}
