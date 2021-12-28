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

package com.ververica.cdc.connectors.base.source.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.internal.connection.JdbcConnectionFactory;
import com.ververica.cdc.connectors.base.source.internal.connection.PooledDataSourceFactory;
import com.ververica.cdc.connectors.base.source.internal.converter.JdbcSourceRecordConverter;
import com.ververica.cdc.connectors.base.source.offset.Offset;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/** Handle the SQL dialect of jdbc driver. */
@Internal
public interface Dialect {
    Logger LOG = LoggerFactory.getLogger(Dialect.class);

    /**
     * Get the name of dialect.
     *
     * @return the dialect name.
     */
    String getName();

    /**
     * Get converter that is used to convert JDBC object to {@link SourceRecord}.
     *
     * @param rowType the given row type.
     * @return a SourceRecord converter for the database.
     */
    JdbcSourceRecordConverter getSourceRecordConverter(RowType rowType);

    /**
     * Get a connection pool factory to create pooled DataSource.
     *
     * @return a connection pool factory.
     */
    PooledDataSourceFactory getPooledDataSourceFactory();

    /**
     * Creates and opens a new {@link JdbcConnection} backing connection pool.
     *
     * @param sourceConfig a basic source configuration.
     * @return a utility that simplifies using a JDBC connection.
     */
    default JdbcConnection openJdbcConnection(SourceConfig sourceConfig) {
        JdbcConnection jdbc =
                new JdbcConnection(
                        sourceConfig.getDbzConfiguration(),
                        new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()));
        try {
            jdbc.connect();
        } catch (Exception e) {
            LOG.error("Failed to open database connection", e);
            throw new FlinkRuntimeException(e);
        }
        return jdbc;
    }

    /**
     * Checks whether split column is evenly distributed across its range.
     *
     * @param splitColumn split column.
     * @return true that means split column with type BIGINT, INT, DECIMAL.
     */
    default boolean isEvenlySplitColumn(Column splitColumn) {
        DataType flinkType = fromDbzColumn(splitColumn);
        LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();

        // currently, we only support the optimization that split column with type BIGINT, INT,
        // DECIMAL
        return typeRoot == LogicalTypeRoot.BIGINT
                || typeRoot == LogicalTypeRoot.INTEGER
                || typeRoot == LogicalTypeRoot.DECIMAL;
    }

    /**
     * convert dbz column to Flink row type.
     *
     * @param splitColumn split column.
     * @return flink row type.
     */
    default RowType getSplitType(Column splitColumn) {
        return (RowType)
                ROW(FIELD(splitColumn.name(), fromDbzColumn(splitColumn))).getLogicalType();
    }

    /**
     * Get a corresponding Flink data type from a debezium {@link Column}.
     *
     * @param splitColumn dbz split column.
     * @return flink data type
     */
    DataType fromDbzColumn(Column splitColumn);

    /**
     * display current offset from the database e.g. query Mysql binary logs by query <code>
     * SHOW MASTER STATUS</code>.
     *
     * @param sourceConfig a basic source configuration.
     * @return current offset of the database.
     */
    Offset displayCurrentOffset(SourceConfig sourceConfig);

    /**
     * discover need captured table schema by {@link SourceConfig}.
     *
     * @param sourceConfig a basic source configuration.
     * @return a map of the {@link TableChanges.TableChange} which are need snapshot or streaming
     *     reading.
     * @throws SQLException when connect to database occur error.
     */
    Map<TableId, TableChanges.TableChange> discoverCapturedTableSchemas(SourceConfig sourceConfig)
            throws SQLException;

    /**
     * discover a list of need captured table.
     *
     * @param sourceConfig a basic source configuration.
     * @return a list of {@link TableId} that is need captured.
     */
    default List<TableId> discoverCapturedTables(SourceConfig sourceConfig) {
        final List<TableId> capturedTableIds;
        try {
            capturedTableIds = listTables(sourceConfig);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to discover captured tables", e);
        }
        if (capturedTableIds.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
                            sourceConfig.getDatabaseList(), sourceConfig.getTableList()));
        }
        return capturedTableIds;
    }

    /**
     * connect to database, fetch all tables from databases, but only return {@link TableId}
     * filtered tables and databases by {@link SourceConfig#getDatabaseList()} {@link
     * SourceConfig#getTableList()}.
     *
     * @param sourceConfig a basic source configuration.
     * @return a list of the {@link TableId} of tables which are need snapshot or streaming reading.
     * @throws SQLException when connect to database occur error
     */
    List<TableId> listTables(SourceConfig sourceConfig) throws SQLException;

    /**
     * Check if the table case sensitive.
     *
     * @param sourceConfig a basic source configuration.
     * @return {@code true} if table case sensitive, {@code false} otherwise.
     */
    boolean isTableIdCaseSensitive(SourceConfig sourceConfig);

    /**
     * Context of the table snapshot or stream reading. Contains result data in {@link
     * ChangeEventQueue}
     */
    interface Context {

        ChangeEventQueue<DataChangeEvent> getQueue();
    }
}
