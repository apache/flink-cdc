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

package org.apache.flink.cdc.connectors.gaussdb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GaussDBScanFetchTask}. */
class GaussDBScanFetchTaskTest {

    @Test
    void testTaskCreation() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        assertThat(task).isNotNull();
        assertThat(task.getSplit()).isEqualTo(snapshotSplit);
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testGetSplit() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        assertThat(task.getSplit()).isEqualTo(snapshotSplit);
        assertThat(task.getSplit().splitId()).isEqualTo("snapshot-split-test");
    }

    @Test
    void testIsRunning() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testClose() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        task.close();

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testMultipleCloseCallsAreSafe() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplit();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        task.close();
        task.close();
        task.close();

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testTaskWithNullSplitStart() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplitWithNullStart();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        assertThat(task).isNotNull();
        assertThat(task.getSplit()).isEqualTo(snapshotSplit);
    }

    @Test
    void testTaskWithNullSplitEnd() {
        SnapshotSplit snapshotSplit = createTestSnapshotSplitWithNullEnd();
        GaussDBScanFetchTask task = new GaussDBScanFetchTask(snapshotSplit);

        assertThat(task).isNotNull();
        assertThat(task.getSplit()).isEqualTo(snapshotSplit);
    }

    private SnapshotSplit createTestSnapshotSplit() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        RowType splitKeyType =
                RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});

        return new SnapshotSplit(
                tableId,
                "snapshot-split-test",
                splitKeyType,
                new Object[] {1L},
                new Object[] {100L},
                null,
                Collections.emptyMap());
    }

    private SnapshotSplit createTestSnapshotSplitWithNullStart() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        RowType splitKeyType =
                RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});

        return new SnapshotSplit(
                tableId,
                "snapshot-split-test-null-start",
                splitKeyType,
                null,
                new Object[] {100L},
                null,
                Collections.emptyMap());
    }

    private SnapshotSplit createTestSnapshotSplitWithNullEnd() {
        TableId tableId = new TableId("testdb", "public", "test_table");
        RowType splitKeyType =
                RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});

        return new SnapshotSplit(
                tableId,
                "snapshot-split-test-null-end",
                splitKeyType,
                new Object[] {1L},
                null,
                null,
                Collections.emptyMap());
    }
}
