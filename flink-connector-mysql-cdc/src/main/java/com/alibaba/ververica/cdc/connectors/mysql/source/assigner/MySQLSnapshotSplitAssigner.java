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
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySQLSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitKind;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition.INITIAL_OFFSET;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.toDebeziumConfig;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.rowToArray;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildMaxPrimaryKeyQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildSplitBoundaryQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.getSplitKey;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.quote;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.readTableSplitStatement;

/** A split assigner that assign table snapshot splits to readers. */
public class MySQLSnapshotSplitAssigner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSnapshotSplitAssigner.class);

    private static final JsonTableChangeSerializer tableChangesSerializer =
            new JsonTableChangeSerializer();
    private final Collection<TableId> alreadyProcessedTables;
    private final Collection<MySQLSplit> remainingSplits;
    private final Configuration configuration;
    private final int splitSize;
    private final RowType splitKeyType;

    private Collection<TableId> capturedTables;
    private TableId currentTableId;
    private int currentTableSplitSeq;
    private Object[] currentTableMaxSplitKey;
    private RelationalTableFilters tableFilters;
    private MySqlConnection jdbc;
    private Map<TableId, SchemaRecord> cachedTableSchemas;
    private MySqlDatabaseSchema databaseSchema;

    public MySQLSnapshotSplitAssigner(
            Configuration configuration,
            RowType pkRowType,
            Collection<TableId> alreadyProcessedTables,
            Collection<MySQLSplit> remainingSplits) {
        this.configuration = configuration;
        this.splitKeyType = getSplitKey(configuration, pkRowType);
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.splitSize = configuration.getInteger(MySQLSourceOptions.SCAN_SPLIT_SIZE);
        Preconditions.checkState(
                splitSize > 1,
                String.format(
                        "The value of option 'scan.split.size' must bigger than 1, but is %d",
                        splitSize));
        this.cachedTableSchemas = new HashMap<>();
    }

    public void open() {
        initJdbcConnection();
        this.tableFilters = getTableFilters();
        this.capturedTables = getCapturedTables();
        this.databaseSchema = StatefulTaskContext.getMySqlDatabaseSchema(configuration, jdbc);
        this.currentTableSplitSeq = 0;
    }

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    public Optional<MySQLSplit> getNext(@Nullable String hostname) {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            MySQLSplit split = remainingSplits.iterator().next();
            remainingSplits.remove(split);
            if (remainingSplits.isEmpty()) {
                this.alreadyProcessedTables.add(currentTableId);
            }
            return Optional.of(split);
        } else {
            // it's turn for new table
            TableId nextTable = getNextTable();
            if (nextTable != null) {
                currentTableId = nextTable;
                currentTableSplitSeq = 0;
                analyzeSplitsForCurrentTable();
                return getNext(hostname);
            } else {
                return Optional.empty();
            }
        }
    }

    private TableId getNextTable() {
        for (TableId tableId : capturedTables) {
            if (!alreadyProcessedTables.contains(tableId)) {
                return tableId;
            }
        }
        return null;
    }

    private void analyzeSplitsForCurrentTable() {
        // TODO: This is the general case, for numeric primary key case,
        //  we can optimize the analysis with numeric range like : 0-1000, 1001-2000...
        MySQLSplit prevSplit = null;
        MySQLSplit nextSplit;
        int splitCnt = 0;
        LOGGER.info("Begin to analyze splits for table {} ", currentTableId);
        while ((nextSplit = getNextSplit(prevSplit)) != null) {
            remainingSplits.add(nextSplit);
            prevSplit = nextSplit;
            splitCnt++;
            if (splitCnt % 100 == 0) {
                try {
                    Thread.sleep(3_000);
                } catch (InterruptedException e) {
                    LOGGER.error(
                            "Interrupted when analyze splits for table {}, exception {}",
                            currentTableId,
                            e);
                }
                LOGGER.info("Has analyze {} splits for table {} ", splitCnt, currentTableId);
            }
        }
        LOGGER.info("Finish to analyze splits for table {} ", currentTableId);

        LOGGER.info("The captured table {} have been assigned success.", currentTableId);
    }

    private MySQLSplit getNextSplit(MySQLSplit prevSplit) {
        boolean isFirstSplit = prevSplit == null;
        boolean isLastSplit = false;
        // first split
        Object[] prevSplitEnd;
        if (isFirstSplit) {
            try {
                currentTableMaxSplitKey =
                        jdbc.queryAndMap(
                                buildMaxPrimaryKeyQuery(currentTableId, splitKeyType),
                                rs -> {
                                    if (!rs.next()) {
                                        return null;
                                    }
                                    return rowToArray(rs, splitKeyType.getFieldCount());
                                });
            } catch (SQLException e) {
                LOGGER.error(
                        String.format("Read max primary key from table %s failed.", currentTableId),
                        e);
            }
            prevSplitEnd = null;
        } else {
            prevSplitEnd = prevSplit.getSplitBoundaryEnd();
            if (Arrays.equals(prevSplitEnd, currentTableMaxSplitKey)) {
                isLastSplit = true;
            }

            // previous split is the last one, no more splits
            if (prevSplitEnd == null) {
                return null;
            }
        }

        Object[] splitEnd = null;
        int stepSize = splitSize;
        while (true) {
            String splitBoundaryQuery =
                    buildSplitBoundaryQuery(
                            currentTableId, splitKeyType, isFirstSplit, isLastSplit, stepSize);
            try (PreparedStatement statement =
                            readTableSplitStatement(
                                    jdbc,
                                    splitBoundaryQuery,
                                    isFirstSplit,
                                    isLastSplit,
                                    currentTableMaxSplitKey,
                                    prevSplitEnd,
                                    splitKeyType.getFieldCount(),
                                    1);
                    ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                splitEnd = isLastSplit ? null : rowToArray(rs, splitKeyType.getFieldCount());
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Read split end of table  " + currentTableId + " failed", e);
            }
            // if the primary key contains multiple field, the the split key may duplicated,
            // try to find a valid key.
            if (isFirstSplit || isLastSplit) {
                break;
            } else if (Arrays.equals(prevSplitEnd, splitEnd)) {
                stepSize = stepSize + splitSize;
                continue;
            } else {
                break;
            }
        }

        Map<TableId, SchemaRecord> databaseHistory = new HashMap<>();
        // cache for optimization
        if (!cachedTableSchemas.containsKey(currentTableId)) {
            cachedTableSchemas.putAll(getTableSchema());
        }
        databaseHistory.put(currentTableId, cachedTableSchemas.get(currentTableId));

        return new MySQLSplit(
                MySQLSplitKind.SNAPSHOT,
                currentTableId,
                createSplitId(),
                splitKeyType,
                prevSplitEnd,
                splitEnd,
                null,
                null,
                false,
                INITIAL_OFFSET,
                new ArrayList<>(),
                databaseHistory);
    }

    private String createSplitId() {
        final String splitId = currentTableId + "-" + currentTableSplitSeq;
        currentTableSplitSeq++;
        return splitId;
    }

    public void close() {
        if (jdbc != null) {
            try {
                jdbc.close();
            } catch (SQLException e) {
                LOGGER.error("Close jdbc connection error", e);
            }
        }
    }

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    public void addSplits(Collection<MySQLSplit> splits) {
        remainingSplits.addAll(splits);
    }

    public Collection<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    /** Gets the remaining splits that this assigner has pending. */
    public Collection<MySQLSplit> remainingSplits() {
        return remainingSplits;
    }

    private Collection<TableId> getCapturedTables() {
        final List<TableId> capturedTableIds = new ArrayList<>();
        try {
            LOGGER.info("Read list of available databases。");
            final List<String> databaseNames = new ArrayList<>();

            jdbc.query(
                    "SHOW DATABASES",
                    rs -> {
                        while (rs.next()) {
                            databaseNames.add(rs.getString(1));
                        }
                    });
            LOGGER.info("The list of available databases is: {}", databaseNames);

            LOGGER.info("Read list of available tables in each database");

            for (String dbName : databaseNames) {
                jdbc.query(
                        "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'",
                        rs -> {
                            while (rs.next()) {
                                TableId tableId = new TableId(dbName, null, rs.getString(1));
                                if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                                    capturedTableIds.add(tableId);
                                    LOGGER.info("Add table '{}' to capture", tableId);
                                } else {
                                    LOGGER.info("Table '{}' is filtered out of capturing", tableId);
                                }
                            }
                        });
            }
        } catch (Exception e) {
            LOGGER.error("Obtain available tables fail.", e);
            this.close();
        }
        return capturedTableIds;
    }

    private RelationalTableFilters getTableFilters() {
        io.debezium.config.Configuration debeziumConfig = toDebeziumConfig(configuration);
        if (configuration.contains(MySQLSourceOptions.DATABASE_NAME)) {
            debeziumConfig =
                    debeziumConfig
                            .edit()
                            .with(
                                    "database.whitelist",
                                    configuration.getString(MySQLSourceOptions.DATABASE_NAME))
                            .build();
        }
        if (configuration.contains(MySQLSourceOptions.TABLE_NAME)) {
            debeziumConfig =
                    debeziumConfig
                            .edit()
                            .with(
                                    "table.whitelist",
                                    configuration.getString(MySQLSourceOptions.TABLE_NAME))
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
            LOGGER.error("connect to mysql error", e);
        }
    }

    private Map<TableId, SchemaRecord> getTableSchema() {
        Map<TableId, SchemaRecord> historyRecords = new HashMap<>();
        try {

            jdbc.query(
                    "SHOW CREATE TABLE " + quote(currentTableId),
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            final MySqlOffsetContext offsetContext =
                                    MySqlOffsetContext.initial(
                                            new MySqlConnectorConfig(
                                                    toDebeziumConfig(configuration)));
                            List<SchemaChangeEvent> schemaChangeEvents =
                                    databaseSchema.parseSnapshotDdl(
                                            ddl,
                                            currentTableId.catalog(),
                                            offsetContext,
                                            Instant.now());
                            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                                for (TableChanges.TableChange tableChange :
                                        schemaChangeEvent.getTableChanges()) {
                                    SchemaRecord schemaRecord =
                                            new SchemaRecord(
                                                    tableChangesSerializer.toDocument(tableChange));
                                    historyRecords.put(currentTableId, schemaRecord);
                                }
                            }
                        }
                    });
        } catch (SQLException e) {
            LOGGER.error("Get table schema error.", e);
        }
        return historyRecords;
    }
}
