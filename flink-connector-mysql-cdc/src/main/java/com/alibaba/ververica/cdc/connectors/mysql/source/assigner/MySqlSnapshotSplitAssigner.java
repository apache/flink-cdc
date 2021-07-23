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

import com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isOptimizedKeyType;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.rowToArray;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SplitKeyUtils.getSplitKeyType;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SplitKeyUtils.splitKeyIsAutoIncremented;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildMaxSplitKeyQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildMinMaxSplitKeyQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.buildSplitBoundaryQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.StatementUtils.readTableSplitStatement;

/** A split assigner that assign table snapshot splits to readers. */
public class MySqlSnapshotSplitAssigner extends MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSnapshotSplitAssigner.class);
    private final int splitSize;

    private TableId currentTableId;
    private int currentTableSplitSeq;
    private Object[] currentTableMaxSplitKey;
    private RowType splitKeyType;

    public MySqlSnapshotSplitAssigner(
            Configuration configuration,
            RowType definedPkType,
            Collection<TableId> alreadyProcessedTables,
            Collection<MySqlSplit> remainingSplits) {
        super(configuration, definedPkType, alreadyProcessedTables, remainingSplits);
        this.splitSize = configuration.getInteger(MySqlSourceOptions.SCAN_SNAPSHOT_CHUNK_SIZE);
        Preconditions.checkState(
                splitSize > 1,
                String.format(
                        "The value of option 'scan.snapshot.chunk.size' must bigger than 1, but is %d",
                        splitSize));
    }

    public void open() {
        super.open();
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
                final TableChange tableChange = getTableSchema(tableId);
                this.tableSchemas.put(tableId, tableChange);
                this.splitKeyType = getSplitKeyType(definedPkType, tableChange.getTable());
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
        // optimization for int/bigint auto_increment type
        final Table currentTable = tableSchemas.get(currentTableId).getTable();
        if (splitKeyIsAutoIncremented(splitKeyType, currentTable)
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
        tableChangeMap.put(currentTableId, tableSchemas.get(currentTableId));

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
        super.close();
    }
}
