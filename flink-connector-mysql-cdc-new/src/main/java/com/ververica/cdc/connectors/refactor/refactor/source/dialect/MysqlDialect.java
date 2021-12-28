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

package com.ververica.cdc.connectors.refactor.refactor.source.dialect;

import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.dialect.Dialect;
import com.ververica.cdc.connectors.base.source.offset.Offset;
import com.ververica.cdc.connectors.refactor.refactor.schema.MySqlSchema;
import com.ververica.cdc.connectors.refactor.refactor.schema.MySqlTypeUtils;
import com.ververica.cdc.connectors.refactor.refactor.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.refactor.refactor.source.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.refactor.refactor.source.utils.TableDiscoveryUtils;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This is the base class for {@link MysqlSnapshotDialect} and {@link MysqlStreamingDialect} . */
public interface MysqlDialect extends Dialect {

    @Override
    default DataType fromDbzColumn(Column splitColumn) {
        return MySqlTypeUtils.fromDbzColumn(splitColumn);
    }

    @Override
    default Offset displayCurrentOffset(SourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return DebeziumUtils.currentBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    default Map<TableId, TableChanges.TableChange> discoverCapturedTableSchemas(
            SourceConfig sourceConfig) throws SQLException {
        final List<TableId> capturedTableIds = discoverCapturedTables(sourceConfig);

        try (MySqlConnection jdbc =
                DebeziumUtils.createMySqlConnection(sourceConfig.getDbzConfiguration())) {
            // fetch table schemas
            MySqlSchema mySqlSchema =
                    new MySqlSchema(
                            (MySqlSourceConfig) sourceConfig, jdbc.isTableIdCaseSensitive());
            Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
            for (TableId tableId : capturedTableIds) {
                TableChanges.TableChange tableSchema = mySqlSchema.getTableSchema(jdbc, tableId);
                tableSchemas.put(tableId, tableSchema);
            }
            return tableSchemas;
        }
    }

    @Override
    default List<TableId> listTables(SourceConfig sourceConfig) throws SQLException {
        MySqlSourceConfig mySqlSourceConfig = (MySqlSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return TableDiscoveryUtils.listTables(
                    jdbcConnection, mySqlSourceConfig.getTableFilters());
        }
    }

    @Override
    default boolean isTableIdCaseSensitive(SourceConfig sourceConfig) {
        try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
            return DebeziumUtils.isTableIdCaseSensitive(jdbcConnection);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading MySQL variables: " + e.getMessage(), e);
        }
    }
}
