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

package org.apache.flink.cdc.connectors.base.source.reader.external;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link IncrementalSourceScanFetcher}. */
class IncrementalSourceScanFetcherTest {

    private static final TableId CURRENT_SPLIT_TABLE = TableId.parse("test_db.table_a");
    private static final TableId OTHER_TABLE = TableId.parse("test_db.table_b");

    /**
     * Reproduces the NPE seen in PostgreSQL backfill when the WAL stream carries change records
     * for a captured table other than the one currently being snapshotted. Before the fix, the
     * fetcher passed the foreign-table record to {@code isRecordBetween}, which dereferenced a
     * null Debezium {@code Table} from the schema cache and threw NPE. After the fix, the
     * foreign-table record is filtered out by tableId and {@code isRecordBetween} is never
     * invoked.
     */
    @Test
    void testIsChangeRecordInChunkRangeFiltersOutForeignTableRecord() {
        FetchTask.Context taskContext = mock(FetchTask.Context.class);
        SourceRecord foreignTableRecord = mock(SourceRecord.class);

        when(taskContext.isDataChangeRecord(foreignTableRecord)).thenReturn(true);
        when(taskContext.getTableId(foreignTableRecord)).thenReturn(OTHER_TABLE);
        // Mirrors the production failure mode: the record is for a table whose schema is not in
        // the cache, so any downstream lookup explodes with NPE. The test asserts we never get
        // here.
        when(taskContext.isRecordBetween(any(), any(), any()))
                .thenThrow(
                        new NullPointerException(
                                "Cannot invoke \"io.debezium.relational.Table.primaryKeyColumns()\" because \"table\" is null"));

        IncrementalSourceScanFetcher fetcher = new IncrementalSourceScanFetcher(taskContext, 0);
        fetcher.setCurrentSnapshotSplit(newSnapshotSplit(CURRENT_SPLIT_TABLE));

        Assertions.assertThatCode(() -> fetcher.isChangeRecordInChunkRange(foreignTableRecord))
                .doesNotThrowAnyException();
        Assertions.assertThat(fetcher.isChangeRecordInChunkRange(foreignTableRecord)).isFalse();
        verify(taskContext, never()).isRecordBetween(any(), any(), any());
    }

    @Test
    void testIsChangeRecordInChunkRangeDelegatesForCurrentTableRecord() {
        FetchTask.Context taskContext = mock(FetchTask.Context.class);
        SourceRecord currentTableRecord = mock(SourceRecord.class);

        when(taskContext.isDataChangeRecord(currentTableRecord)).thenReturn(true);
        when(taskContext.getTableId(currentTableRecord)).thenReturn(CURRENT_SPLIT_TABLE);
        when(taskContext.isRecordBetween(any(), any(), any())).thenReturn(true);

        IncrementalSourceScanFetcher fetcher = new IncrementalSourceScanFetcher(taskContext, 0);
        fetcher.setCurrentSnapshotSplit(newSnapshotSplit(CURRENT_SPLIT_TABLE));

        Assertions.assertThat(fetcher.isChangeRecordInChunkRange(currentTableRecord)).isTrue();
        verify(taskContext).isRecordBetween(any(), any(), any());
    }

    @Test
    void testIsChangeRecordInChunkRangeIgnoresNonDataChangeRecord() {
        FetchTask.Context taskContext = mock(FetchTask.Context.class);
        SourceRecord watermarkRecord = mock(SourceRecord.class);

        when(taskContext.isDataChangeRecord(watermarkRecord)).thenReturn(false);

        IncrementalSourceScanFetcher fetcher = new IncrementalSourceScanFetcher(taskContext, 0);
        fetcher.setCurrentSnapshotSplit(newSnapshotSplit(CURRENT_SPLIT_TABLE));

        Assertions.assertThat(fetcher.isChangeRecordInChunkRange(watermarkRecord)).isFalse();
        verify(taskContext, never()).getTableId(any());
        verify(taskContext, never()).isRecordBetween(any(), any(), any());
    }

    private static SnapshotSplit newSnapshotSplit(TableId tableId) {
        RowType splitKeyType =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()))
                                .getLogicalType();
        return new SnapshotSplit(
                tableId,
                0,
                splitKeyType,
                new Object[] {0L},
                new Object[] {1024L},
                null,
                Collections.emptyMap());
    }
}
