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

package com.alibaba.ververica.cdc.connectors.mysql.source.assigner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.toDebeziumConfig;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.quote;

/** A split assigner that assign table snapshot split or table binlog split to readers. */
public abstract class MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitAssigner.class);
    protected final Configuration configuration;
    protected final RowType definedPkType;
    protected final Collection<TableId> alreadyProcessedTables;
    protected final Collection<MySqlSplit> remainingSplits;

    protected Collection<TableId> capturedTables;
    protected RelationalTableFilters tableFilters;
    protected MySqlConnection jdbc;
    protected Map<TableId, TableChange> tableSchemas;
    protected MySqlDatabaseSchema databaseSchema;

    public MySqlSplitAssigner(
            Configuration configuration,
            RowType definedPkType,
            Collection<TableId> alreadyProcessedTables,
            Collection<MySqlSplit> remainingSplits) {
        this.configuration = configuration;
        this.definedPkType = definedPkType;
        this.tableSchemas = new HashMap<>();
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
    }

    /** Checks whether this assigner is a snapshot assigner. */
    public final boolean isSnapshotSplitAssigner() {
        return getClass() == MySqlSnapshotSplitAssigner.class;
    }

    /** Checks whether this assigner is a binlog assigner. */
    public final boolean isBinlogSplitAssigner() {
        return getClass() == MySqlBinlogSplitAssigner.class;
    }

    /** Casts this assigner into a {@link MySqlSnapshotSplitAssigner}. */
    @SuppressWarnings("unchecked")
    public final MySqlSnapshotSplitAssigner asSnapshotSplitAssigner() {
        return (MySqlSnapshotSplitAssigner) this;
    }

    /** Casts this assigner into a {@link MySqlBinlogSplitAssigner}. */
    @SuppressWarnings("unchecked")
    public final MySqlBinlogSplitAssigner asBinlogSplitAssigner() {
        return (MySqlBinlogSplitAssigner) this;
    }

    public void open() {
        initJdbcConnection();
        this.tableFilters = getTableFilters();
        this.capturedTables = getCapturedTables();
        this.databaseSchema = StatefulTaskContext.getMySqlDatabaseSchema(configuration, jdbc);
    }

    /** Gets the next split, returns null when no more splits. */
    public abstract Optional<MySqlSplit> getNext(@Nullable String hostname);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    public void addSplits(Collection<MySqlSplit> splits) {
        remainingSplits.addAll(splits);
    }

    public Collection<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    /** Gets the remaining splits that this assigner has pending. */
    public Collection<MySqlSplit> remainingSplits() {
        return remainingSplits;
    }

    public void close() {
        if (jdbc != null) {
            try {
                jdbc.close();
            } catch (SQLException e) {
                LOG.error("Close jdbc connection error", e);
                throw new FlinkRuntimeException("Close jdbc connection error", e);
            }
        }
    }

    protected Collection<TableId> getCapturedTables() {
        final List<TableId> capturedTableIds = new ArrayList<>();
        try {
            LOG.info("Read list of available databases。");
            final List<String> databaseNames = new ArrayList<>();

            jdbc.query(
                    "SHOW DATABASES",
                    rs -> {
                        while (rs.next()) {
                            databaseNames.add(rs.getString(1));
                        }
                    });
            LOG.info("The list of available databases is: {}", databaseNames);

            LOG.info("Read list of available tables in each database");

            for (String dbName : databaseNames) {
                jdbc.query(
                        "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'",
                        rs -> {
                            while (rs.next()) {
                                TableId tableId = new TableId(dbName, null, rs.getString(1));
                                if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                                    capturedTableIds.add(tableId);
                                    LOG.info("Add table '{}' to capture", tableId);
                                } else {
                                    LOG.info("Table '{}' is filtered out of capturing", tableId);
                                }
                            }
                        });
            }
        } catch (Exception e) {
            LOG.error("Obtain available tables fail.", e);
            throw new FlinkRuntimeException("Obtain available tables fail.", e);
        }
        Preconditions.checkState(
                capturedTableIds.size() > 0,
                String.format(
                        "The captured table(s) is empty!, please check your configured table-name %s",
                        configuration.get(MySqlSourceOptions.TABLE_NAME)));
        return capturedTableIds;
    }

    protected RelationalTableFilters getTableFilters() {
        io.debezium.config.Configuration debeziumConfig = toDebeziumConfig(configuration);
        if (configuration.contains(MySqlSourceOptions.DATABASE_NAME)) {
            debeziumConfig =
                    debeziumConfig
                            .edit()
                            .with(
                                    "database.whitelist",
                                    configuration.getString(MySqlSourceOptions.DATABASE_NAME))
                            .build();
        }
        if (configuration.contains(MySqlSourceOptions.TABLE_NAME)) {
            debeziumConfig =
                    debeziumConfig
                            .edit()
                            .with(
                                    "table.whitelist",
                                    configuration.getString(MySqlSourceOptions.TABLE_NAME))
                            .build();
        }
        final MySqlConnectorConfig mySqlConnectorConfig = new MySqlConnectorConfig(debeziumConfig);
        return mySqlConnectorConfig.getTableFilters();
    }

    private void initJdbcConnection() {

        this.jdbc = StatefulTaskContext.getConnection(configuration);
        try {
            jdbc.connect();
        } catch (SQLException e) {
            LOG.error("connect to mysql error", e);
            throw new FlinkRuntimeException("connect to mysql error", e);
        }
    }

    protected TableChange getTableSchema(final TableId tableId) {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        try {
            jdbc.query(
                    "SHOW CREATE TABLE " + quote(tableId),
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            final MySqlOffsetContext offsetContext =
                                    MySqlOffsetContext.initial(
                                            new MySqlConnectorConfig(
                                                    toDebeziumConfig(configuration)));
                            List<SchemaChangeEvent> schemaChangeEvents =
                                    databaseSchema.parseSnapshotDdl(
                                            ddl, tableId.catalog(), offsetContext, Instant.now());
                            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                                for (TableChange tableChange :
                                        schemaChangeEvent.getTableChanges()) {
                                    tableChangeMap.put(tableId, tableChange);
                                }
                            }
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Get schema of table %s error.", tableId), e);
        }
        if (tableChangeMap.isEmpty()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Can not get schema of table %s, please check the configured table name.",
                            tableId));
        }

        return tableChangeMap.get(tableId);
    }
}
