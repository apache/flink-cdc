/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.experimental;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.FlinkRuntimeException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.experimental.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.base.experimental.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.base.experimental.fetch.MySqlScanFetchTask;
import com.ververica.cdc.connectors.base.experimental.fetch.MySqlSourceFetchTaskContext;
import com.ververica.cdc.connectors.base.experimental.fetch.MySqlStreamFetchTask;
import com.ververica.cdc.connectors.base.experimental.utils.MySqlSchema;
import com.ververica.cdc.connectors.base.experimental.utils.TableDiscoveryUtils;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import com.ververica.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.base.experimental.utils.MySqlConnectionUtils.createBinaryClient;
import static com.ververica.cdc.connectors.base.experimental.utils.MySqlConnectionUtils.createMySqlConnection;
import static com.ververica.cdc.connectors.base.experimental.utils.MySqlConnectionUtils.currentBinlogOffset;
import static com.ververica.cdc.connectors.base.experimental.utils.MySqlConnectionUtils.isTableIdCaseSensitive;

/** The {@link JdbcDataSourceDialect} implementation for MySQL datasource. */
@Experimental
public class MySqlDialect implements JdbcDataSourceDialect {

    private static final long serialVersionUID = 1L;
    private final MySqlSourceConfigFactory configFactory;
    private final MySqlSourceConfig sourceConfig;
    private transient MySqlSchema mySqlSchema;

    public MySqlDialect(MySqlSourceConfigFactory configFactory) {
        this.configFactory = configFactory;
        this.sourceConfig = configFactory.create(0);
    }

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return currentBinlogOffset(jdbcConnection);
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
    public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
        return new MySqlChunkSplitter(sourceConfig, this);
    }

    @Override
    public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
        return new MysqlPooledDataSourceFactory();
    }

    @Override
    public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
        MySqlSourceConfig mySqlSourceConfig = (MySqlSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, mySqlSourceConfig.getTableFilters());
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
        final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

        try (MySqlConnection jdbc = createMySqlConnection(sourceConfig.getDbzConfiguration())) {
            // fetch table schemas
            Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChanges.TableChange tableSchema = queryTableSchema(jdbc, tableId);
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
        if (mySqlSchema == null) {
            mySqlSchema =
                    new MySqlSchema(sourceConfig, isDataCollectionIdCaseSensitive(sourceConfig));
        }
        return mySqlSchema.getTableSchema(jdbc, tableId);
    }

    @Override
    public MySqlSourceFetchTaskContext createFetchTaskContext(
            SourceSplitBase sourceSplitBase, JdbcSourceConfig taskSourceConfig) {
        final MySqlConnection jdbcConnection =
                createMySqlConnection(taskSourceConfig.getDbzConfiguration());
        final BinaryLogClient binaryLogClient =
                createBinaryClient(taskSourceConfig.getDbzConfiguration());
        return new MySqlSourceFetchTaskContext(
                taskSourceConfig, this, jdbcConnection, binaryLogClient);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new MySqlScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new MySqlStreamFetchTask(sourceSplitBase.asStreamSplit());
        }
    }
}
