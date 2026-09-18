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

package org.apache.flink.cdc.connectors.fluss.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussDefaultDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussSubscriberTableDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.event.FinishedKvSnapshotConsumeEvent;
import org.apache.flink.cdc.connectors.fluss.source.event.TableRemovalAckEvent;
import org.apache.flink.cdc.connectors.fluss.source.event.TableSubscriptionEvent;
import org.apache.flink.cdc.connectors.fluss.source.reader.LeaseContext;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanUtils;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FlussSourceEnumerator} focusing on dynamic table discovery via both {@link
 * FlussDefaultDiscoverer} and {@link FlussSubscriberTableDiscoverer}.
 *
 * <p>Each test drives the enumerator manually through one or more discovery cycles using {@link
 * MockSplitEnumeratorContext#runPeriodicCallable(int)} + {@link
 * MockSplitEnumeratorContext#runNextOneTimeCallable()}, and asserts on the split assignments
 * emitted for each cycle.
 */
class FlussSourceEnumeratorTest {

    /** Index of the periodic discovery callable scheduled by {@link FlussSourceEnumerator}. */
    private static final int DISCOVERY_CALLABLE_INDEX = 0;

    private static final int NUM_READERS = 2;
    private static final long DISCOVERY_INTERVAL_MS = Duration.ofSeconds(10).toMillis();
    private static final String DATABASE_NAME = "enum_test_db";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    private TableEnvironment tBatchEnv;

    @BeforeEach
    void before() throws Exception {
        waitForFlussClusterReady();
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "CREATE CATALOG test_catalog WITH ('type' = 'fluss', '%s' = '%s')",
                        BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("USE CATALOG test_catalog");
        tBatchEnv.executeSql("CREATE DATABASE " + DATABASE_NAME);
        tBatchEnv.useDatabase(DATABASE_NAME);
    }

    @AfterEach
    void after() {
        tBatchEnv.useDatabase(BUILTIN_DATABASE);
        tBatchEnv.executeSql(String.format("DROP DATABASE %s CASCADE", DATABASE_NAME));
    }

    // =====================================================================
    //  FlussDefaultDiscoverer tests — regex-based matching and dynamic discovery
    // =====================================================================

    /**
     * Tests that {@link FlussDefaultDiscoverer} only assigns tables whose fully-qualified name
     * matches the supplied regex, and leaves non-matching tables completely unassigned.
     */
    @Test
    void testPatternSubscriberOnlyAssignsMatchingTables() throws Throwable {
        String tableA = "match_a";
        String tableB = "match_b";
        String tableOther = "other_c";
        createPkTable(tableA);
        createPkTable(tableB);
        createPkTable(tableOther);

        FlussDefaultDiscoverer discoverer = new FlussDefaultDiscoverer();
        String pattern = fqnRegex(DATABASE_NAME, "match_.*");

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, discoverer, pattern);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                runDiscoveryCycle(context);

                assertThat(assignedTableNames(context))
                        .containsExactlyInAnyOrder(tableA, tableB)
                        .doesNotContain(tableOther);
            } finally {
                enumerator.close();
            }
        }
    }

    /**
     * Tests that {@link FlussDefaultDiscoverer} discovers newly created tables matching the pattern
     * on the next periodic discovery cycle, and emits splits only for the new tables.
     */
    @Test
    void testPatternSubscriberDiscoversNewTableDynamically() throws Throwable {
        String tableA = "dyn_a";
        createPkTable(tableA);

        FlussDefaultDiscoverer discoverer = new FlussDefaultDiscoverer();
        String pattern = fqnRegex(DATABASE_NAME, "dyn_.*");

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, discoverer, pattern);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                // First cycle: tableA is discovered.
                runDiscoveryCycle(context);
                assertThat(assignedTableNames(context)).containsExactly(tableA);
                int assignmentsAfterFirst = context.getSplitsAssignmentSequence().size();

                // A new table matching the pattern is created between cycles.
                String tableB = "dyn_b";
                createPkTable(tableB);

                // Second cycle: tableB should be discovered and assigned.
                runDiscoveryCycle(context);

                // A brand-new splits-assignment should have been emitted for tableB only.
                assertThat(context.getSplitsAssignmentSequence().size())
                        .isEqualTo(assignmentsAfterFirst + 1);
                assertThat(latestAssignmentTableNames(context)).containsExactly(tableB);

                // Accumulated assigned table set includes both.
                assertThat(assignedTableNames(context)).containsExactlyInAnyOrder(tableA, tableB);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testPatternSubscriberRemovesDroppedTableAfterReaderAcknowledgements() throws Throwable {
        String tableA = "rm_a";
        String tableB = "rm_b";
        createPkTable(tableA);
        createPkTable(tableB);

        FlussDefaultDiscoverer discoverer = new FlussDefaultDiscoverer();
        String pattern = fqnRegex(DATABASE_NAME, "rm_.*");

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, discoverer, pattern);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                runDiscoveryCycle(context);
                assertThat(assignedTableNames(context)).containsExactlyInAnyOrder(tableA, tableB);
                int assignmentsAfterFirst = context.getSplitsAssignmentSequence().size();

                tBatchEnv.executeSql(String.format("DROP TABLE %s", tableB)).await();
                runDiscoveryCycle(context);

                assertThat(context.getSplitsAssignmentSequence()).hasSize(assignmentsAfterFirst);
                TablePath removedTablePath = TablePath.of(DATABASE_NAME, tableB);
                TableSubscriptionEvent removal = latestSubscriptionEvent(context, 0);
                assertThat(removal.getSubscribedTablePaths())
                        .containsExactly(TablePath.of(DATABASE_NAME, tableA));
                assertThat(removal.getPendingRemovalRequests()).containsOnlyKeys(removedTablePath);
                long requestId = removal.getPendingRemovalRequests().get(removedTablePath);
                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths())
                        .containsExactly(removedTablePath);

                acknowledgeRemoval(enumerator, removedTablePath, requestId);
                assertThat(enumerator.snapshotState(2L).getPendingRemovalTablePaths()).isEmpty();
                assertThat(latestSubscriptionEvent(context, 0).getSubscribedTablePaths())
                        .containsExactly(TablePath.of(DATABASE_NAME, tableA));
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testRemovalFenceSurvivesAbortedCheckpointAndRearm() throws Throwable {
        String tableA = "fence_a";
        String tableB = "fence_b";
        createPkTable(tableA);
        createPkTable(tableB);

        FlussDefaultDiscoverer discoverer = new FlussDefaultDiscoverer();
        String pattern = fqnRegex(DATABASE_NAME, "fence_.*");

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, discoverer, pattern);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);

                TablePath tablePath = TablePath.of(DATABASE_NAME, tableB);
                tBatchEnv.executeSql(String.format("DROP TABLE %s", tableB)).await();
                runDiscoveryCycle(context);
                long firstRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                acknowledgeRemoval(enumerator, tablePath, firstRequestId);
                assertThat(latestSubscriptionEvent(context, 0).getFencedTablePaths())
                        .contains(tablePath);
                enumerator.snapshotState(1L);
                enumerator.addReader(0);
                assertThat(latestSubscriptionEvent(context, 0).getFencedTablePaths())
                        .contains(tablePath);
                enumerator.snapshotState(2L);
                enumerator.notifyCheckpointComplete(2L);
                enumerator.addReader(0);
                assertThat(latestSubscriptionEvent(context, 0).getFencedTablePaths())
                        .doesNotContain(tablePath);

                createPkTable(tableB);
                runDiscoveryCycle(context);
                tBatchEnv.executeSql(String.format("DROP TABLE %s", tableB)).await();
                runDiscoveryCycle(context);
                long secondRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                acknowledgeRemoval(enumerator, tablePath, secondRequestId);
                assertThat(latestSubscriptionEvent(context, 0).getFencedTablePaths())
                        .contains(tablePath);
                enumerator.snapshotState(3L);

                createPkTable(tableB);
                runDiscoveryCycle(context);
                tBatchEnv.executeSql(String.format("DROP TABLE %s", tableB)).await();
                runDiscoveryCycle(context);
                long thirdRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                acknowledgeRemoval(enumerator, tablePath, thirdRequestId);

                enumerator.notifyCheckpointComplete(3L);
                enumerator.addReader(0);
                assertThat(latestSubscriptionEvent(context, 0).getFencedTablePaths())
                        .contains(tablePath);

                enumerator.snapshotState(4L);
                enumerator.notifyCheckpointComplete(4L);
                enumerator.addReader(0);
                assertThat(latestSubscriptionEvent(context, 0).getFencedTablePaths())
                        .doesNotContain(tablePath);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testFencedAddSplitsBackDropsStaleSplitWithoutReassigningOtherBuckets() throws Throwable {
        String subscriptionTable = "sub_fenced_failed_split";
        String targetTable = "tgt_fenced_failed_split";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable, 2);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);

                TablePath tablePath = TablePath.of(DATABASE_NAME, targetTable);
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                runDiscoveryCycle(context);
                long requestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                acknowledgeRemoval(enumerator, tablePath, requestId);

                insertSubscription(subscriptionTable, targetTable);
                int assignmentsBeforeReadd = context.getSplitsAssignmentSequence().size();
                runDiscoveryCycle(context);
                List<FlussSplitBase> freshSplits =
                        context
                                .getSplitsAssignmentSequence()
                                .subList(
                                        assignmentsBeforeReadd,
                                        context.getSplitsAssignmentSequence().size())
                                .stream()
                                .flatMap(assignment -> assignment.assignment().values().stream())
                                .flatMap(List::stream)
                                .collect(Collectors.toList());
                assertThat(freshSplits).hasSize(2);
                FlussLogSplit freshB0 =
                        freshSplits.stream()
                                .filter(
                                        split ->
                                                FlussSourceEnumerator.getSplitOwner(
                                                                split.getTableBucket(), NUM_READERS)
                                                        == 0)
                                .map(FlussSplitBase::asLogSplit)
                                .findFirst()
                                .orElseThrow(AssertionError::new);
                assertThat(
                                freshSplits.stream()
                                        .filter(
                                                split ->
                                                        FlussSourceEnumerator.getSplitOwner(
                                                                        split.getTableBucket(),
                                                                        NUM_READERS)
                                                                == 1))
                        .singleElement();

                FlussLogSplit staleB0 =
                        new FlussLogSplit(
                                freshB0.getPhysicalTablePath(),
                                freshB0.getTableBucket(),
                                freshB0.getStartingOffset() + 1);
                int assignmentsBeforeFailedReader = context.getSplitsAssignmentSequence().size();
                enumerator.addSplitsBack(List.of(staleB0, freshB0), 0);

                assertThat(context.getSplitsAssignmentSequence())
                        .hasSize(assignmentsBeforeFailedReader + 1);
                SplitsAssignment<FlussSplitBase> failedReaderAssignment =
                        context.getSplitsAssignmentSequence()
                                .get(context.getSplitsAssignmentSequence().size() - 1);
                assertThat(failedReaderAssignment.assignment()).containsOnlyKeys(0);
                assertThat(failedReaderAssignment.assignment().get(0)).containsExactly(freshB0);

                int assignmentsBeforeNextDiscovery = context.getSplitsAssignmentSequence().size();
                runDiscoveryCycle(context);
                assertThat(context.getSplitsAssignmentSequence())
                        .hasSize(assignmentsBeforeNextDiscovery);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testDroppedPartitionDoesNotCreateTableRemovalTombstone() throws Throwable {
        String tableName = "partition_still_subscribed";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, ds STRING, val STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED) PARTITIONED BY (ds)",
                                tableName))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, '20260904', 'first'), (2, '20260905', 'second')",
                                tableName))
                .await();

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                        new MockSplitEnumeratorContext<>(NUM_READERS);
                Connection connection =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context,
                            new FlussDefaultDiscoverer(),
                            fqnRegex(DATABASE_NAME, tableName));
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);

                TablePath tablePath = TablePath.of(DATABASE_NAME, tableName);
                connection
                        .getAdmin()
                        .dropPartition(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("ds", "20260904")),
                                false)
                        .get();
                runDiscoveryCycle(context);

                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths()).isEmpty();
                TableSubscriptionEvent subscription = latestSubscriptionEvent(context, 0);
                assertThat(subscription.getSubscribedTablePaths()).containsExactly(tablePath);
                assertThat(subscription.getPendingRemovalRequests()).isEmpty();

                tBatchEnv.executeSql(String.format("DROP TABLE %s", tableName)).await();
                runDiscoveryCycle(context);

                TableSubscriptionEvent removal = latestSubscriptionEvent(context, 0);
                assertThat(enumerator.snapshotState(2L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);
                assertThat(removal.getPendingRemovalRequests()).containsKey(tablePath);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testDiscoveryFailsWhenTableDiscoveryFails() throws Throwable {
        RuntimeException discoveryFailure =
                new RuntimeException("Injected table discovery failure");
        TableDiscoverer failingDiscoverer =
                new TableDiscoverer() {
                    @Override
                    public void open(Context context) {}

                    @Override
                    public Set<TableId> discover() {
                        throw discoveryFailure;
                    }

                    @Override
                    public void close() {}
                };

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context, failingDiscoverer, null, OffsetsInitializer.earliest(), 0L);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                assertThatThrownBy(context::runNextOneTimeCallable)
                        .isInstanceOf(FlinkRuntimeException.class)
                        .hasMessage("Failed to discover subscribed table-buckets.")
                        .hasRootCauseMessage("Injected table discovery failure");
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testDiscoveryFailsWhenOffsetInitializationFails() throws Throwable {
        String tableName = "offset_failure";
        createPkTable(tableName);

        RuntimeException offsetFailure =
                new RuntimeException("Injected offset initialization failure");
        OffsetsInitializer failingOffsetsInitializer =
                (partitionName, bucketIds, retriever) -> {
                    throw offsetFailure;
                };

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context,
                            new FlussDefaultDiscoverer(),
                            fqnRegex(DATABASE_NAME, tableName),
                            failingOffsetsInitializer,
                            0L);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                context.runNextOneTimeCallable();
                assertThat(context.getOneTimeCallables()).hasSize(1);

                assertThatThrownBy(context::runNextOneTimeCallable)
                        .isInstanceOf(FlinkRuntimeException.class)
                        .hasMessage("Failed to initialize splits for new table-buckets.")
                        .hasRootCauseMessage("Injected offset initialization failure");
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testDiscoveryFailsWhenOffsetInitializationIsIncomplete() throws Throwable {
        String tableName = "incomplete_offsets";
        createPkTable(tableName);

        OffsetsInitializer incompleteOffsetsInitializer =
                (partitionName, bucketIds, retriever) -> Collections.emptyMap();

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context,
                            new FlussDefaultDiscoverer(),
                            fqnRegex(DATABASE_NAME, tableName),
                            incompleteOffsetsInitializer,
                            0L);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                context.runNextOneTimeCallable();
                assertThat(context.getOneTimeCallables()).hasSize(1);

                assertThatThrownBy(context::runNextOneTimeCallable)
                        .isInstanceOf(FlinkRuntimeException.class)
                        .hasMessage("Failed to initialize splits for new table-buckets.")
                        .rootCause()
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(
                                "Offsets initializer did not return offsets for buckets");
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
            } finally {
                enumerator.close();
            }
        }
    }

    // =====================================================================
    //  Enumerator checkpoint restore tests
    // =====================================================================

    @Test
    void testRestorePendingLatestSplitPreservesStartingOffsetAfterRescale() throws Throwable {
        String tableName = "restore_pending_latest";
        createPkTable(tableName);
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'before-checkpoint')", tableName))
                .await();

        AtomicInteger offsetInitializationCount = new AtomicInteger();
        OffsetsInitializer latestOffsetsInitializer = OffsetsInitializer.latest();
        OffsetsInitializer trackingLatestOffsetsInitializer =
                (partitionName, bucketIds, retriever) -> {
                    offsetInitializationCount.incrementAndGet();
                    return latestOffsetsInitializer.getBucketOffsets(
                            partitionName, bucketIds, retriever);
                };

        FlussSourceEnumState checkpoint;
        FlussSplitBase checkpointedSplit;
        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context,
                            new FlussDefaultDiscoverer(),
                            fqnRegex(DATABASE_NAME, tableName),
                            trackingLatestOffsetsInitializer,
                            DISCOVERY_INTERVAL_MS);
            try {
                enumerator.start();

                runDiscoveryCycle(context);
                checkpoint = enumerator.snapshotState(1L);

                assertThat(checkpoint.getAssignedPhysicalTablePaths()).isEmpty();
                assertThat(checkpoint.getRemainingSplits()).hasSize(1);
                checkpointedSplit = checkpoint.getRemainingSplits().get(0);
                assertThat(checkpointedSplit.isLogSplit()).isTrue();
                assertThat(checkpointedSplit.asLogSplit().getStartingOffset()).isEqualTo(1L);
                assertThat(offsetInitializationCount.get()).isEqualTo(1);
            } finally {
                enumerator.close();
            }
        }

        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (2, 'after-checkpoint')", tableName))
                .await();

        int originalOwner =
                FlussSourceEnumerator.getSplitOwner(
                        checkpointedSplit.getTableBucket(), NUM_READERS);
        int restoredParallelism =
                findParallelismWithDifferentOwner(
                        checkpointedSplit.getTableBucket(), NUM_READERS, originalOwner);
        int restoredOwner =
                FlussSourceEnumerator.getSplitOwner(
                        checkpointedSplit.getTableBucket(), restoredParallelism);

        try (MockSplitEnumeratorContext<FlussSplitBase> restoredContext =
                new MockSplitEnumeratorContext<>(restoredParallelism)) {
            org.apache.fluss.config.Configuration flussConfig =
                    FLUSS_CLUSTER_EXTENSION.getClientConfig();
            FlussSourceEnumerator restoredEnumerator =
                    new FlussSourceEnumerator(
                            restoredContext,
                            new FlussDefaultDiscoverer(),
                            flussConfig,
                            buildSourceConfig(flussConfig, fqnRegex(DATABASE_NAME, tableName)),
                            trackingLatestOffsetsInitializer,
                            DISCOVERY_INTERVAL_MS,
                            checkpoint,
                            LeaseContext.fromConf(
                                    new org.apache.flink.configuration.Configuration()));
            try {
                restoredEnumerator.start();

                // Discovery may run before the restored split's owner reader registers.
                runDiscoveryCycle(restoredContext);

                restoredContext.registerReader(
                        new ReaderInfo(originalOwner, "loc_" + originalOwner));
                restoredEnumerator.addReader(originalOwner);
                assertThat(restoredContext.getSplitsAssignmentSequence()).isEmpty();

                restoredContext.registerReader(
                        new ReaderInfo(restoredOwner, "loc_" + restoredOwner));
                restoredEnumerator.addReader(restoredOwner);

                assertThat(restoredOwner).isNotEqualTo(originalOwner);
                assertThat(restoredContext.getSplitsAssignmentSequence()).hasSize(1);
                SplitsAssignment<FlussSplitBase> assignment =
                        restoredContext.getSplitsAssignmentSequence().get(0);
                assertThat(assignment.assignment()).containsOnlyKeys(restoredOwner);
                assertThat(assignment.assignment().get(restoredOwner))
                        .containsExactly(checkpointedSplit);
                assertThat(offsetInitializationCount.get()).isEqualTo(1);
            } finally {
                restoredEnumerator.close();
            }
        }
    }

    @Test
    void testRestoreHybridSplitKeepsCheckpointedSnapshotAvailable() throws Throwable {
        String tableName = "restore_snapshot_lease";
        createPkTable(tableName);
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'checkpointed')", tableName))
                .await();

        TablePath tablePath = TablePath.of(DATABASE_NAME, tableName);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        FlussSourceEnumState checkpoint;
        FlussHybridSnapshotLogSplit checkpointedSplit;
        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context,
                            new FlussDefaultDiscoverer(),
                            fqnRegex(DATABASE_NAME, tableName),
                            OffsetsInitializer.full(),
                            DISCOVERY_INTERVAL_MS);
            try {
                enumerator.start();
                runDiscoveryCycle(context);

                checkpoint = enumerator.snapshotState(1L);
                assertThat(checkpoint.getRemainingSplits()).hasSize(1);
                assertThat(checkpoint.getRemainingSplits().get(0).isHybridSnapshotLogSplit())
                        .isTrue();
                checkpointedSplit =
                        checkpoint.getRemainingSplits().get(0).asHybridSnapshotLogSplit();

                enumerator.notifyCheckpointComplete(1L);
            } finally {
                enumerator.close();
            }
        }

        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (2, 'newer-snapshot-1')", tableName))
                .await();
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (3, 'newer-snapshot-2')", tableName))
                .await();
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        try (MockSplitEnumeratorContext<FlussSplitBase> restoredContext =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            org.apache.fluss.config.Configuration flussConfig =
                    FLUSS_CLUSTER_EXTENSION.getClientConfig();
            FlussSourceEnumerator restoredEnumerator =
                    new FlussSourceEnumerator(
                            restoredContext,
                            new FlussDefaultDiscoverer(),
                            flussConfig,
                            buildSourceConfig(flussConfig, fqnRegex(DATABASE_NAME, tableName)),
                            OffsetsInitializer.full(),
                            DISCOVERY_INTERVAL_MS,
                            checkpoint,
                            LeaseContext.fromConf(
                                    new org.apache.flink.configuration.Configuration()));
            try {
                restoredEnumerator.start();
                runDiscoveryCycle(restoredContext);

                int owner =
                        FlussSourceEnumerator.getSplitOwner(
                                checkpointedSplit.getTableBucket(), NUM_READERS);
                restoredContext.registerReader(new ReaderInfo(owner, "loc_" + owner));
                restoredEnumerator.addReader(owner);

                assertThat(restoredContext.getSplitsAssignmentSequence()).hasSize(1);
                FlussHybridSnapshotLogSplit restoredSplit =
                        restoredContext
                                .getSplitsAssignmentSequence()
                                .get(0)
                                .assignment()
                                .get(owner)
                                .get(0)
                                .asHybridSnapshotLogSplit();
                assertThat(restoredSplit.getSnapshotId())
                        .isEqualTo(checkpointedSplit.getSnapshotId());

                List<InternalRow> rows;
                try (Connection scanConnection =
                                ConnectionFactory.createConnection(
                                        FLUSS_CLUSTER_EXTENSION.getClientConfig());
                        Table table = scanConnection.getTable(restoredSplit.getTablePath());
                        BatchScanner scanner =
                                table.newScan()
                                        .createBatchScanner(
                                                restoredSplit.getTableBucket(),
                                                restoredSplit.getSnapshotId())) {
                    rows = BatchScanUtils.collectRows(scanner);
                }
                assertThat(rows).hasSize(1);
            } finally {
                restoredEnumerator.close();
            }
        }
    }

    // =====================================================================
    //  FlussTableSubscriber tests — subscription-table driven add/remove
    // =====================================================================

    /**
     * Tests that {@link FlussSubscriberTableDiscoverer} assigns exactly the tables initially seeded
     * into the subscription table.
     */
    @Test
    void testFlussTableSubscriberInitialDiscovery() throws Throwable {
        String subscriptionTable = "sub_initial";
        String targetA = "tgt_initial_a";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetA);
        insertSubscription(subscriptionTable, targetA);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                runDiscoveryCycle(context);

                assertThat(assignedTableNames(context)).containsExactly(targetA);
            } finally {
                enumerator.close();
            }
        }
    }

    /**
     * Tests that inserting a new row into the subscription table causes the enumerator to discover
     * and assign the corresponding newly-created target table on the next periodic discovery.
     */
    @Test
    void testFlussTableSubscriberDynamicallyAddsTable() throws Throwable {
        String subscriptionTable = "sub_add";
        String targetA = "tgt_add_a";
        String targetB = "tgt_add_b";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetA);
        createPkTable(targetB);
        insertSubscription(subscriptionTable, targetA);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                // First cycle: only targetA is subscribed.
                runDiscoveryCycle(context);
                assertThat(assignedTableNames(context)).containsExactly(targetA);
                int assignmentsAfterFirst = context.getSplitsAssignmentSequence().size();

                // Dynamically add targetB to subscription.
                insertSubscription(subscriptionTable, targetB);

                // Second cycle: targetB should be discovered and assigned.
                runDiscoveryCycle(context);

                assertThat(context.getSplitsAssignmentSequence().size())
                        .isEqualTo(assignmentsAfterFirst + 1);
                assertThat(latestAssignmentTableNames(context)).containsExactly(targetB);
                assertThat(assignedTableNames(context)).containsExactlyInAnyOrder(targetA, targetB);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testSubscriptionDeletionPersistsTombstoneUntilEveryReaderAcknowledges() throws Throwable {
        String subscriptionTable = "sub_delete";
        String targetTable = "tgt_delete";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                runDiscoveryCycle(context);
                assertThat(assignedTableNames(context)).containsExactly(targetTable);
                int assignmentsAfterFirst = context.getSplitsAssignmentSequence().size();
                FlussSplitBase assignedSplit =
                        context.getSplitsAssignmentSequence().get(0).assignment().values().stream()
                                .flatMap(List::stream)
                                .findFirst()
                                .orElseThrow(AssertionError::new);

                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();

                runDiscoveryCycle(context);

                assertThat(context.getSplitsAssignmentSequence()).hasSize(assignmentsAfterFirst);
                TablePath tablePath = TablePath.of(DATABASE_NAME, targetTable);
                TableSubscriptionEvent removal = latestSubscriptionEvent(context, 0);
                assertThat(removal.getSubscribedTablePaths()).isEmpty();
                assertThat(removal.getPendingRemovalRequests()).containsOnlyKeys(tablePath);
                long firstRequestId = removal.getPendingRemovalRequests().get(tablePath);
                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);

                enumerator.addSplitsBack(
                        Collections.singletonList(assignedSplit),
                        FlussSourceEnumerator.getSplitOwner(
                                assignedSplit.getTableBucket(), NUM_READERS));
                assertThat(context.getSplitsAssignmentSequence()).hasSize(assignmentsAfterFirst);

                enumerator.handleSourceEvent(
                        0,
                        new TableRemovalAckEvent(
                                Collections.singletonMap(tablePath, firstRequestId - 1)));
                assertThat(enumerator.snapshotState(2L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);

                enumerator.addReader(0);
                long restartedRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                assertThat(restartedRequestId).isNotEqualTo(firstRequestId);

                insertSubscription(subscriptionTable, targetTable);
                runDiscoveryCycle(context);
                assertThat(context.getSplitsAssignmentSequence()).hasSize(assignmentsAfterFirst);
                assertThat(latestSubscriptionEvent(context, 0).getSubscribedTablePaths())
                        .containsExactly(tablePath);
                assertThat(latestSubscriptionEvent(context, 0).getPendingRemovalRequests())
                        .containsOnlyKeys(tablePath);

                acknowledgeRemoval(enumerator, tablePath, firstRequestId);
                assertThat(enumerator.snapshotState(3L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);

                acknowledgeRemoval(enumerator, tablePath, restartedRequestId);
                assertThat(enumerator.snapshotState(4L).getPendingRemovalTablePaths()).isEmpty();
                context.runNextOneTimeCallable();
                assertThat(context.getSplitsAssignmentSequence())
                        .hasSize(assignmentsAfterFirst + 1);
                assertThat(latestAssignmentTableNames(context)).containsExactly(targetTable);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testRemovalWaitsForRegisteredReadersAndDropsLateFailedSplit() throws Throwable {
        String subscriptionTable = "sub_reader_restart";
        String targetTable = "tgt_reader_restart";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);
                FlussSplitBase failedSplit =
                        context.getSplitsAssignmentSequence().get(0).assignment().values().stream()
                                .flatMap(List::stream)
                                .findFirst()
                                .orElseThrow(AssertionError::new);
                TablePath tablePath = TablePath.of(DATABASE_NAME, targetTable);

                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                runDiscoveryCycle(context);
                long oldRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);

                enumerator.handleSourceEvent(
                        0,
                        new TableRemovalAckEvent(
                                Collections.singletonMap(tablePath, oldRequestId)));
                context.unregisterReader(0);
                enumerator.handleSourceEvent(
                        1,
                        new TableRemovalAckEvent(
                                Collections.singletonMap(tablePath, oldRequestId)));
                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);

                context.registerReader(new ReaderInfo(0, "restarted_0"));
                enumerator.addReader(0);
                long restartedRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                assertThat(restartedRequestId).isNotEqualTo(oldRequestId);

                acknowledgeRemoval(enumerator, tablePath, oldRequestId);
                assertThat(enumerator.snapshotState(2L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);

                acknowledgeRemoval(enumerator, tablePath, restartedRequestId);
                assertThat(enumerator.snapshotState(3L).getPendingRemovalTablePaths()).isEmpty();

                int assignmentsBeforeLateSplit = context.getSplitsAssignmentSequence().size();
                enumerator.addSplitsBack(
                        Collections.singletonList(failedSplit),
                        FlussSourceEnumerator.getSplitOwner(
                                failedSplit.getTableBucket(), NUM_READERS));
                assertThat(context.getSplitsAssignmentSequence())
                        .hasSize(assignmentsBeforeLateSplit);
                FlussSourceEnumState state = enumerator.snapshotState(4L);
                assertThat(state.getAssignedPhysicalTablePaths())
                        .noneMatch(path -> path.getTablePath().equals(tablePath));
                assertThat(state.getRemainingSplits())
                        .noneMatch(split -> split.getTablePath().equals(tablePath));
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testLateInitializationDoesNotReviveDeletedSubscription() throws Throwable {
        String subscriptionTable = "sub_late_init";
        String targetTable = "tgt_late_init";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);
                context.runNextOneTimeCallable();

                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths())
                        .containsExactly(TablePath.of(DATABASE_NAME, targetTable));
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testLateFailedInitializationAfterRemovalAcknowledgementIsIgnored() throws Throwable {
        String subscriptionTable = "sub_late_failed_init";
        String targetTable = "tgt_late_failed_init";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        RuntimeException offsetFailure = new RuntimeException("Injected late offset failure");
        OffsetsInitializer failingOffsetsInitializer =
                (partitionName, bucketIds, retriever) -> {
                    throw offsetFailure;
                };
        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newEnumerator(
                            context,
                            subscriber,
                            null,
                            failingOffsetsInitializer,
                            DISCOVERY_INTERVAL_MS);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);

                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);

                TablePath tablePath = TablePath.of(DATABASE_NAME, targetTable);
                long requestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                acknowledgeRemoval(enumerator, tablePath, requestId);

                assertThatCode(context::runNextOneTimeCallable).doesNotThrowAnyException();
                assertThat(context.getSplitsAssignmentSequence()).isEmpty();
                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths()).isEmpty();
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testRestoreRegeneratesRemovalRequestIdAndRejectsOldAcknowledgement() throws Throwable {
        String subscriptionTable = "sub_restore";
        String targetTable = "tgt_restore";
        TablePath tablePath = TablePath.of(DATABASE_NAME, targetTable);
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);
        FlussSourceEnumState restoredState;
        long oldRequestId;
        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);

                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                runDiscoveryCycle(context);
                oldRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                restoredState = enumerator.snapshotState(1L);
            } finally {
                enumerator.close();
            }
        }

        insertSubscription(subscriptionTable, targetTable);
        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newRestoredEnumerator(
                            context,
                            new FlussSubscriberTableDiscoverer(
                                    DATABASE_NAME + "." + subscriptionTable, 100),
                            restoredState);
            try {
                enumerator.start();
                runDiscoveryCycle(context);
                registerAllReaders(context, enumerator);

                long restoredRequestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(tablePath);
                assertThat(restoredRequestId).isNotEqualTo(oldRequestId);
                assertThat(enumerator.snapshotState(2L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);

                acknowledgeRemoval(enumerator, tablePath, oldRequestId);
                assertThat(enumerator.snapshotState(3L).getPendingRemovalTablePaths())
                        .containsExactly(tablePath);
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testRestartedReaderWaitsForFreshDiscoveryBeforeReceivingRemovalSnapshot()
            throws Throwable {
        String subscriptionTable = "sub_restore_fresh_discovery";
        String targetTable = "tgt_restore_fresh_discovery";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);
        FlussSourceEnumState restoredState;
        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);

                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                runDiscoveryCycle(context);
                restoredState = enumerator.snapshotState(1L);
            } finally {
                enumerator.close();
            }
        }

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator =
                    newRestoredEnumerator(
                            context,
                            new FlussSubscriberTableDiscoverer(
                                    DATABASE_NAME + "." + subscriptionTable, 100),
                            restoredState);
            try {
                enumerator.start();
                context.registerReader(new ReaderInfo(0, "loc_0"));
                enumerator.addReader(0);
                context.unregisterReader(0);
                context.registerReader(new ReaderInfo(0, "loc_0_restarted"));
                enumerator.addReader(0);

                assertThat(context.getSentSourceEvent()).doesNotContainKey(0);

                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);

                assertThat(latestSubscriptionEvent(context, 0).getPendingRemovalRequests())
                        .containsKey(TablePath.of(DATABASE_NAME, targetTable));
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testMixedInitializationCallbackClearsAcknowledgedRemovalTombstone() throws Throwable {
        String subscriptionTable = "sub_mixed_init";
        String removedTable = "tgt_mixed_removed";
        String retainedTable = "tgt_mixed_retained";
        createSubscriptionTable(subscriptionTable);
        createPkTable(removedTable);
        createPkTable(retainedTable);
        insertSubscription(subscriptionTable, removedTable);
        insertSubscription(subscriptionTable, retainedTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, removedTable))
                        .await();
                context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);

                TablePath removedTablePath = TablePath.of(DATABASE_NAME, removedTable);
                long requestId =
                        latestSubscriptionEvent(context, 0)
                                .getPendingRemovalRequests()
                                .get(removedTablePath);
                acknowledgeRemoval(enumerator, removedTablePath, requestId);
                assertThat(enumerator.snapshotState(1L).getPendingRemovalTablePaths())
                        .containsExactly(removedTablePath);

                context.runNextOneTimeCallable();

                assertThat(latestAssignmentTableNames(context)).containsExactly(retainedTable);
                assertThat(enumerator.snapshotState(2L).getPendingRemovalTablePaths()).isEmpty();
            } finally {
                enumerator.close();
            }
        }
    }

    @Test
    void testSubscriptionDeletionRemovesPendingKvSnapshotRelease() throws Throwable {
        String subscriptionTable = "sub_snapshot_release";
        String targetTable = "tgt_snapshot_release";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetTable);
        insertSubscription(subscriptionTable, targetTable);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);
                runDiscoveryCycle(context);

                FlussSplitBase split =
                        context.getSplitsAssignmentSequence().get(0).assignment().values().stream()
                                .flatMap(List::stream)
                                .findFirst()
                                .orElseThrow(AssertionError::new);
                enumerator.handleSourceEvent(
                        0,
                        new FinishedKvSnapshotConsumeEvent(
                                1L, Collections.singleton(split.getTableBucket())));
                assertThat(enumerator.pendingKvSnapshotBucketsForTesting())
                        .containsExactly(split.getTableBucket());

                tBatchEnv
                        .executeSql(
                                String.format(
                                        "DELETE FROM %s WHERE table_name = '%s.%s'",
                                        subscriptionTable, DATABASE_NAME, targetTable))
                        .await();
                runDiscoveryCycle(context);

                assertThat(enumerator.pendingKvSnapshotBucketsForTesting()).isEmpty();
                enumerator.handleSourceEvent(
                        0,
                        new FinishedKvSnapshotConsumeEvent(
                                1L, Collections.singleton(split.getTableBucket())));
                assertThat(enumerator.pendingKvSnapshotBucketsForTesting()).isEmpty();
            } finally {
                enumerator.close();
            }
        }
    }

    // =====================================================================
    //  Helpers
    // =====================================================================

    private FlussSourceEnumerator newEnumerator(
            MockSplitEnumeratorContext<FlussSplitBase> context,
            TableDiscoverer discoverer,
            String pattern) {
        return newEnumerator(
                context, discoverer, pattern, OffsetsInitializer.earliest(), DISCOVERY_INTERVAL_MS);
    }

    private FlussSourceEnumerator newEnumerator(
            MockSplitEnumeratorContext<FlussSplitBase> context,
            TableDiscoverer discoverer,
            String pattern,
            OffsetsInitializer offsetsInitializer,
            long discoveryIntervalMs) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Configuration sourceConfig = buildSourceConfig(flussConfig, pattern);
        return new FlussSourceEnumerator(
                context,
                discoverer,
                flussConfig,
                sourceConfig,
                offsetsInitializer,
                discoveryIntervalMs,
                new HashSet<>(),
                Collections.emptyList(),
                LeaseContext.fromConf(new org.apache.flink.configuration.Configuration()),
                false);
    }

    private FlussSourceEnumerator newRestoredEnumerator(
            MockSplitEnumeratorContext<FlussSplitBase> context,
            TableDiscoverer discoverer,
            FlussSourceEnumState restoredState) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        return new FlussSourceEnumerator(
                context,
                discoverer,
                flussConfig,
                buildSourceConfig(flussConfig, null),
                OffsetsInitializer.earliest(),
                DISCOVERY_INTERVAL_MS,
                restoredState,
                LeaseContext.fromConf(new org.apache.flink.configuration.Configuration()));
    }

    private static Configuration buildSourceConfig(
            org.apache.fluss.config.Configuration flussConfig, String pattern) {
        Map<String, String> map = new HashMap<>();
        String bootstrapServers =
                flussConfig
                        .toMap()
                        .get(org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (bootstrapServers != null) {
            map.put("bootstrap.servers", bootstrapServers);
        }
        if (pattern != null) {
            map.put("table.discoverer.pattern", pattern);
        }
        flussConfig
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith("client.")) {
                                map.put("properties." + key, value);
                            }
                        });
        return Configuration.fromMap(map);
    }

    private static void registerAllReaders(
            MockSplitEnumeratorContext<FlussSplitBase> context, FlussSourceEnumerator enumerator) {
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            context.registerReader(new ReaderInfo(readerId, "loc_" + readerId));
            enumerator.addReader(readerId);
        }
    }

    private static void acknowledgeRemoval(
            FlussSourceEnumerator enumerator, TablePath tablePath, long requestId) {
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            enumerator.handleSourceEvent(
                    readerId,
                    new TableRemovalAckEvent(Collections.singletonMap(tablePath, requestId)));
        }
    }

    private static int findParallelismWithDifferentOwner(
            org.apache.fluss.metadata.TableBucket tableBucket,
            int currentParallelism,
            int currentOwner) {
        for (int parallelism = currentParallelism + 1;
                parallelism <= currentParallelism + 10;
                parallelism++) {
            if (FlussSourceEnumerator.getSplitOwner(tableBucket, parallelism) != currentOwner) {
                return parallelism;
            }
        }
        throw new IllegalStateException(
                "Could not find a parallelism with a different split owner");
    }

    /**
     * Drives one full discovery cycle: runs the periodic callable (phase 1 + 2) and, if new
     * table-buckets were discovered, the follow-up one-time callable (phase 3 + 4).
     */
    private static void runDiscoveryCycle(MockSplitEnumeratorContext<FlussSplitBase> context)
            throws Throwable {
        context.runPeriodicCallable(DISCOVERY_CALLABLE_INDEX);
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    /** Returns the set of all table names ever assigned across every emitted assignment. */
    private static Set<String> assignedTableNames(
            MockSplitEnumeratorContext<FlussSplitBase> context) {
        return context.getSplitsAssignmentSequence().stream()
                .flatMap(assignment -> assignment.assignment().values().stream())
                .flatMap(List::stream)
                .map(split -> split.getPhysicalTablePath().getTableName())
                .collect(Collectors.toSet());
    }

    /** Returns the set of table names in the most recent assignment only. */
    private static Set<String> latestAssignmentTableNames(
            MockSplitEnumeratorContext<FlussSplitBase> context) {
        List<SplitsAssignment<FlussSplitBase>> sequence = context.getSplitsAssignmentSequence();
        if (sequence.isEmpty()) {
            return Collections.emptySet();
        }
        SplitsAssignment<FlussSplitBase> last = sequence.get(sequence.size() - 1);
        return last.assignment().values().stream()
                .flatMap(List::stream)
                .map(split -> split.getPhysicalTablePath().getTableName())
                .collect(Collectors.toSet());
    }

    private static TableSubscriptionEvent latestSubscriptionEvent(
            MockSplitEnumeratorContext<FlussSplitBase> context, int readerId) throws Exception {
        return context.getSentSourceEvent().get(readerId).stream()
                .filter(TableSubscriptionEvent.class::isInstance)
                .map(TableSubscriptionEvent.class::cast)
                .reduce((first, second) -> second)
                .orElseThrow(AssertionError::new);
    }

    private void createPkTable(String tableName) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();
    }

    private void createPkTable(String tableName, int bucketCount) throws Exception {
        TablePath tablePath = TablePath.of(DATABASE_NAME, tableName);
        try (Connection connection =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            connection
                    .getAdmin()
                    .createTable(
                            tablePath,
                            TableDescriptor.builder()
                                    .schema(
                                            org.apache.fluss.metadata.Schema.newBuilder()
                                                    .column("id", DataTypes.INT())
                                                    .column("val", DataTypes.STRING())
                                                    .primaryKey("id")
                                                    .build())
                                    .distributedBy(bucketCount, "id")
                                    .build(),
                            false)
                    .get();
        }
    }

    private void createSubscriptionTable(String tableName) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (table_name STRING, PRIMARY KEY (table_name) NOT ENFORCED)",
                                tableName))
                .await();
    }

    private void insertSubscription(String subscriptionTable, String targetTable) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('%s.%s')",
                                subscriptionTable, DATABASE_NAME, targetTable))
                .await();
    }

    private static String fqnRegex(String database, String tablePattern) {
        return Pattern.quote(database) + "\\." + tablePattern;
    }

    private void waitForFlussClusterReady() throws Exception {
        int maxRetries = 30;
        int retryIntervalMs = 1000;
        Exception lastException = null;
        for (int i = 0; i < maxRetries; i++) {
            try (Connection connection =
                    ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
                return;
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(retryIntervalMs);
            }
        }
        throw new IllegalStateException(
                "Failed to connect to Fluss cluster after " + maxRetries + " attempts",
                lastException);
    }

    private static org.apache.fluss.config.Configuration initConfig() {
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofHours(1));
        conf.setInt(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, 1);
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        return conf;
    }
}
