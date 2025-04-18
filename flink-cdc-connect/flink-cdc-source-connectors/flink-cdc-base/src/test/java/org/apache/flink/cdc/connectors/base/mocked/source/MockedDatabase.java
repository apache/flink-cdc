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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

/** A dummy database that supports generating snapshot & split data. */
public class MockedDatabase implements Serializable {

    private final long tableCount;
    private final long recordCount;
    private final Duration refreshInterval;
    private static volatile List<Tuple2<TableId, GenericRowData>> mockedEvents;
    private static volatile long currentOffset;
    private @Nullable Thread thread;

    public MockedDatabase(long tableCount, long recordCount, Duration refreshInterval) {
        this.tableCount = tableCount;
        this.recordCount = recordCount;
        this.refreshInterval = refreshInterval;
    }

    private List<Tuple2<TableId, GenericRowData>> getMockedEvents() {
        if (mockedEvents == null) {
            mockedEvents = generateMockedEvents(tableCount, recordCount);
        }
        return mockedEvents;
    }

    public long getCurrentOffset() {
        synchronized (this) {
            return currentOffset;
        }
    }

    public List<TableId> getTableIds() {
        return generateTableIds(tableCount);
    }

    public Map<TableId, TableChanges.TableChange> getTableIdsAndSchema() {
        return getTableIds().stream()
                .collect(Collectors.toMap(Function.identity(), this::getSchema));
    }

    public Map<TableId, TableChanges.TableChange> retrieveSchemas(List<TableId> tableIds) {
        return tableIds.stream().collect(Collectors.toMap(Function.identity(), this::getSchema));
    }

    private TableChanges.TableChange getSchema(TableId tableId) {
        Table table =
                Table.editor()
                        .tableId(tableId)
                        .addColumn(Column.editor().name("id").optional(false).create())
                        .addColumn(Column.editor().name("desc").optional(true).create())
                        .setPrimaryKeyNames("id")
                        .create();
        return new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
    }

    public Tuple2<Long, List<RowData>> takeSnapshot(TableId tableId, long minRange, long maxRange) {
        long currentOffset = getCurrentOffset();
        Map<Long, GenericRowData> snapshot = new HashMap<>();
        getMockedEvents().stream()
                .limit(currentOffset)
                .filter(rec -> rec.f0.equals(tableId))
                .map(rec -> rec.f1)
                .forEach(
                        rowData -> {
                            RowKind rowKind = rowData.getRowKind();
                            long pkValue = rowData.getLong(0);
                            if (pkValue < minRange || pkValue >= maxRange) {
                                return;
                            }
                            if (rowKind == INSERT || rowKind == UPDATE_AFTER) {
                                snapshot.put(pkValue, rowData);
                            } else if (rowKind == UPDATE_BEFORE || rowKind == DELETE) {
                                snapshot.remove(pkValue);
                            }
                        });
        return Tuple2.of(currentOffset, new ArrayList<>(snapshot.values()));
    }

    public Tuple2<Long, List<Tuple3<TableId, Long, GenericRowData>>> pollStreamValues(
            @Nullable TableId tableId,
            long sinceOffset,
            long endingOffset,
            @Nullable Integer splitIndex,
            int totalSplit) {
        synchronized (this) {
            long maxOffset = Math.min(endingOffset, getCurrentOffset());

            List<Tuple2<TableId, GenericRowData>> events = getMockedEvents();
            List<Tuple3<TableId, Long, GenericRowData>> producedEvents = new ArrayList<>();

            for (long i = sinceOffset; i < Math.min(maxOffset, events.size()); i++) {
                Tuple2<TableId, GenericRowData> evt = events.get((int) i);
                if (tableId != null && !tableId.equals(evt.f0)) {
                    continue;
                }
                long primaryKey = evt.f1.getLong(0);
                if (splitIndex != null && Math.floorMod(primaryKey, totalSplit) != splitIndex) {
                    continue;
                }
                producedEvents.add(Tuple3.of(evt.f0, i, evt.f1));
            }

            return Tuple2.of(maxOffset, producedEvents);
        }
    }

    public long count(TableId tableId) {
        return takeSnapshot(tableId, Long.MIN_VALUE, Long.MAX_VALUE).f1.size();
    }

    public void start(long startingOffset) {
        currentOffset = startingOffset;
        thread =
                new Thread(
                        () -> {
                            while (true) {
                                synchronized (this) {
                                    currentOffset++;
                                }
                                try {
                                    Thread.sleep(refreshInterval.toMillis());
                                } catch (InterruptedException e) {
                                    // Gracefully stop
                                    return;
                                }
                            }
                        });
        thread.start();
    }

    public void stop() {
        if (thread != null) {
            thread.interrupt();
        }
        currentOffset = 0;
    }

    private List<TableId> generateTableIds(long tableCount) {
        return LongStream.rangeClosed(1, tableCount)
                .mapToObj(tableId -> new TableId("mock_ns", "mock_scm", "table_" + tableId))
                .collect(Collectors.toList());
    }

    private List<Tuple2<TableId, GenericRowData>> generateMockedEvents(
            long tableCount, long recordCount) {
        List<TableId> tableIds = generateTableIds(tableCount);
        List<Tuple2<TableId, GenericRowData>> generatedRowData = new ArrayList<>();
        for (long i = 1; i < recordCount + 1; i++) {
            final long index = i;
            tableIds.forEach(
                    tableId ->
                            generatedRowData.add(
                                    Tuple2.of(
                                            tableId,
                                            GenericRowData.ofKind(
                                                    INSERT, index, wrap("INITIAL_" + tableId)))));
        }
        for (long i = 1; i < recordCount + 1; i++) {
            final long index = -i;
            tableIds.forEach(
                    tableId -> {
                        generatedRowData.add(
                                Tuple2.of(
                                        tableId,
                                        GenericRowData.ofKind(
                                                INSERT, index, wrap("INSERT_" + tableId))));
                        generatedRowData.add(
                                Tuple2.of(
                                        tableId,
                                        GenericRowData.ofKind(
                                                UPDATE_BEFORE, index, wrap("INSERT_" + tableId))));
                        generatedRowData.add(
                                Tuple2.of(
                                        tableId,
                                        GenericRowData.ofKind(
                                                UPDATE_AFTER, index, wrap("UPDATE_" + tableId))));
                        generatedRowData.add(
                                Tuple2.of(
                                        tableId,
                                        GenericRowData.ofKind(
                                                DELETE, index, wrap("UPDATE_" + tableId))));
                    });
        }
        return generatedRowData;
    }

    private static StringData wrap(String str) {
        return StringData.fromString(str);
    }

    public static List<SourceSplitBase> createStreamSplits(
            MockedConfig config,
            Offset minOffset,
            Offset stoppingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended,
            boolean isSnapshotCompleted) {
        if (stoppingOffset.isAtOrAfter(MockedOffset.MAX)
                && config.isMultipleStreamSplitsEnabled()) {
            List<SourceSplitBase> splits = new ArrayList<>();

            for (int i = 0; i < 4; i++) {
                splits.add(
                        new StreamSplit(
                                STREAM_SPLIT_ID + "-" + i,
                                minOffset,
                                stoppingOffset,
                                finishedSnapshotSplitInfos,
                                tableSchemas,
                                totalFinishedSplitSize,
                                isSuspended,
                                isSnapshotCompleted));
            }

            return splits;
        } else {
            return Collections.singletonList(
                    new StreamSplit(
                            STREAM_SPLIT_ID,
                            minOffset,
                            stoppingOffset,
                            finishedSnapshotSplitInfos,
                            tableSchemas,
                            totalFinishedSplitSize,
                            isSuspended,
                            isSnapshotCompleted));
        }
    }
}
