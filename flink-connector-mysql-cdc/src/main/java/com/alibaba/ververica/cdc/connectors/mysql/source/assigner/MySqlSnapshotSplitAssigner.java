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
import com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.toDebeziumConfig;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isOptimizedKeyType;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.rowToArray;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildMaxSplitKeyQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildMinMaxSplitKeyQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildSplitBoundaryQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.getSplitKey;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.quote;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.readTableSplitStatement;

/** A split assigner that assign table snapshot splits to readers. */
public class MySqlSnapshotSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSnapshotSplitAssigner.class);

    private static final JsonTableChangeSerializer tableChangesSerializer =
            new JsonTableChangeSerializer();
    private final Collection<TableId> alreadyProcessedTables;
    private final Collection<MySqlSplit> remainingSplits;
    private final Configuration configuration;
    private final int splitSize;
    private final RowType splitKeyType;
    private final boolean enableIntegralOptimization;

    private Collection<TableId> capturedTables;
    private TableId currentTableId;
    private int currentTableSplitSeq;
    private Object[] currentTableMaxSplitKey;
    private RelationalTableFilters tableFilters;
    private MySqlConnection jdbc;
    private Map<TableId, TableChange> cachedTableSchemas;
    private MySqlDatabaseSchema databaseSchema;

    public MySqlSnapshotSplitAssigner(
            Configuration configuration,
            RowType pkRowType,
            Collection<TableId> alreadyProcessedTables,
            Collection<MySqlSplit> remainingSplits) {
        this.configuration = configuration;
        this.splitKeyType = getSplitKey(configuration, pkRowType);
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.splitSize = configuration.getInteger(MySqlSourceOptions.SCAN_SPLIT_SIZE);
        Preconditions.checkState(
                splitSize > 1,
                String.format(
                        "The value of option 'scan.split.size' must bigger than 1, but is %d",
                        splitSize));
        this.enableIntegralOptimization =
                configuration.getBoolean(MySqlSourceOptions.SCAN_OPTIMIZE_INTEGRAL_KEY);
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
    public Optional<MySqlSplit> getNext(@Nullable String hostname) {
        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            MySqlSplit split = remainingSplits.iterator().next();
            remainingSplits.remove(split);
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
        MySqlSplit prevSplit = null;
        MySqlSplit nextSplit;
        int splitCnt = 0;
        long start = System.currentTimeMillis();
        List<MySqlSplit> splitsForCurrentTable = new ArrayList<>();
        LOG.info("Begin to analyze splits for table {} ", currentTableId);
        // optimization for integral, bigDecimal type
        if (enableIntegralOptimization
                && isOptimizedKeyType(splitKeyType.getTypeAt(0).getTypeRoot())) {
            String splitKeyFieldName = splitKeyType.getFieldNames().get(0);
            Object[] minMaxSplitKey = new Object[2];
            try {
                minMaxSplitKey =
                        jdbc.queryAndMap(
                                buildMinMaxSplitKeyQuery(currentTableId, splitKeyFieldName),
                                rs -> {
                                    if (!rs.next()) {
                                        return null;
                                    }
                                    return rowToArray(rs, 2);
                                });
            } catch (SQLException e) {
                LOG.error(
                        String.format(
                                "Read max value and min value of split key from table %s failed.",
                                currentTableId),
                        e);
            }
            Object prevSplitEnd = null;
            do {
                Object splitEnd = getOptimizedSplitEnd(prevSplitEnd, minMaxSplitKey);
                splitsForCurrentTable.add(
                        createSnapshotSplit(
                                prevSplitEnd == null ? null : new Object[] {prevSplitEnd},
                                splitEnd == null ? null : new Object[] {splitEnd}));
                prevSplitEnd = splitEnd;
            } while (prevSplitEnd != null);
        }
        // general case
        else {
            while ((nextSplit = getNextSplit(prevSplit)) != null) {
                splitsForCurrentTable.add(nextSplit);
                prevSplit = nextSplit;
                splitCnt++;
                if (splitCnt % 100 == 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOG.error(
                                "Interrupted when analyze splits for table {}, exception {}",
                                currentTableId,
                                e);
                    }
                    LOG.info("Has analyze {} splits for table {} ", splitCnt, currentTableId);
                }
            }
        }

        alreadyProcessedTables.add(currentTableId);
        remainingSplits.addAll(splitsForCurrentTable);

        long end = System.currentTimeMillis();
        LOG.info(
                "Finish to analyze splits for table {}, time cost:{} ",
                currentTableId,
                Duration.ofMillis(end - start));
    }

    public Object getOptimizedSplitEnd(Object prevSplitEnd, Object[] minMaxSplitKey) {
        // first split
        if (prevSplitEnd == null) {
            return minMaxSplitKey[0];
        }
        // last split
        else if (prevSplitEnd.equals(minMaxSplitKey[1])) {
            return null;
        } else {
            if (prevSplitEnd instanceof Integer) {
                return Math.min(
                        ((Integer) prevSplitEnd + splitSize), ((Integer) minMaxSplitKey[1]));
            } else if (prevSplitEnd instanceof Long) {
                return Math.min(((Long) prevSplitEnd + splitSize), ((Long) minMaxSplitKey[1]));
            }
            // BigDecimal
            else if (prevSplitEnd instanceof BigInteger) {
                BigInteger splitEnd =
                        ((BigInteger) prevSplitEnd).add(BigInteger.valueOf(splitSize));
                BigInteger splitMax = ((BigInteger) minMaxSplitKey[1]);
                return splitEnd.compareTo(splitMax) >= 0 ? splitMax : splitEnd;
            } else if (prevSplitEnd instanceof BigDecimal) {
                BigDecimal splitEnd =
                        ((BigDecimal) prevSplitEnd).add(BigDecimal.valueOf(splitSize));
                BigDecimal splitMax = ((BigDecimal) minMaxSplitKey[1]);
                return splitEnd.compareTo(splitMax) >= 0 ? splitMax : splitEnd;
            } else {
                throw new IllegalStateException("Unsupported type for numeric split optimization");
            }
        }
    }

    private MySqlSplit getNextSplit(MySqlSplit prevSplit) {
        boolean isFirstSplit = prevSplit == null;
        boolean isLastSplit = false;
        // first split
        Object[] prevSplitEnd;
        if (isFirstSplit) {
            try {
                currentTableMaxSplitKey =
                        jdbc.queryAndMap(
                                buildMaxSplitKeyQuery(currentTableId, splitKeyType),
                                rs -> {
                                    if (!rs.next()) {
                                        return null;
                                    }
                                    return rowToArray(rs, splitKeyType.getFieldCount());
                                });
            } catch (SQLException e) {
                LOG.error(
                        String.format("Read max primary key from table %s failed.", currentTableId),
                        e);
            }
            prevSplitEnd = null;
        } else {
            prevSplitEnd = prevSplit.asSnapshotSplit().getSplitEnd();
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
        return createSnapshotSplit(prevSplitEnd, splitEnd);
    }

    private MySqlSplit createSnapshotSplit(Object[] splitStart, Object[] splitEnd) {
        Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        // cache for optimization
        if (!cachedTableSchemas.containsKey(currentTableId)) {
            cachedTableSchemas.putAll(getTableSchema());
        }
        tableChangeMap.put(currentTableId, cachedTableSchemas.get(currentTableId));

        return new MySqlSnapshotSplit(
                currentTableId,
                createSplitId(),
                splitKeyType,
                splitStart,
                splitEnd,
                null,
                tableChangeMap);
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
                LOG.error("Close jdbc connection error", e);
            }
        }
    }

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

    private Collection<TableId> getCapturedTables() {
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
            this.close();
        }
        Preconditions.checkState(
                capturedTableIds.size() > 0,
                String.format(
                        "The captured table(s) is empty!, please check your configured table-name %s",
                        configuration.get(MySqlSourceOptions.TABLE_NAME)));
        return capturedTableIds;
    }

    private RelationalTableFilters getTableFilters() {
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
        }
    }

    private Map<TableId, TableChange> getTableSchema() {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
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
                                for (TableChange tableChange :
                                        schemaChangeEvent.getTableChanges()) {
                                    tableChangeMap.put(currentTableId, tableChange);
                                }
                            }
                        }
                    });
        } catch (SQLException e) {
            LOG.error("Get table schema error.", e);
        }
        return tableChangeMap;
    }
}
