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
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussDefaultDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.discover.FlussSubscriberTableDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

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

    /**
     * Tests that when a subscribed table is dropped, the enumerator does not emit any new
     * assignments on the following discovery cycle (current enumerator intentionally does not
     * revoke already-assigned tables).
     */
    @Test
    void testPatternSubscriberIgnoresTableRemoval() throws Throwable {
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

                // First cycle: both tables assigned.
                runDiscoveryCycle(context);
                assertThat(assignedTableNames(context)).containsExactlyInAnyOrder(tableA, tableB);
                int assignmentsAfterFirst = context.getSplitsAssignmentSequence().size();

                // Drop tableB — pattern no longer matches it.
                tBatchEnv.executeSql(String.format("DROP TABLE %s", tableB)).await();

                // Second cycle: no new assignments should be emitted. The enumerator keeps its
                // previously-assigned state (no revocation support yet).
                runDiscoveryCycle(context);

                assertThat(context.getSplitsAssignmentSequence())
                        .as("Shrinking subscription should not emit new assignments")
                        .hasSize(assignmentsAfterFirst);
            } finally {
                enumerator.close();
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

    /**
     * Tests that removing a row from the subscription table does NOT cause the enumerator to emit
     * new assignments or revoke any splits on the next discovery cycle — the current enumerator
     * intentionally does not revoke already-assigned tables.
     */
    @Test
    void testFlussTableSubscriberIgnoresSubscriptionShrinkage() throws Throwable {
        String subscriptionTable = "sub_shrink";
        String targetA = "tgt_shrink_a";
        String targetB = "tgt_shrink_b";
        createSubscriptionTable(subscriptionTable);
        createPkTable(targetA);
        createPkTable(targetB);
        insertSubscription(subscriptionTable, targetA);
        insertSubscription(subscriptionTable, targetB);

        FlussSubscriberTableDiscoverer subscriber =
                new FlussSubscriberTableDiscoverer(DATABASE_NAME + "." + subscriptionTable, 100);

        try (MockSplitEnumeratorContext<FlussSplitBase> context =
                new MockSplitEnumeratorContext<>(NUM_READERS)) {
            FlussSourceEnumerator enumerator = newEnumerator(context, subscriber, null);
            try {
                enumerator.start();
                registerAllReaders(context, enumerator);

                // First cycle: both tables assigned.
                runDiscoveryCycle(context);
                assertThat(assignedTableNames(context)).containsExactlyInAnyOrder(targetA, targetB);
                int assignmentsAfterFirst = context.getSplitsAssignmentSequence().size();

                // Shrink the subscription by dropping & recreating the subscription table with
                // only targetA. (Using DROP+CREATE avoids relying on SQL DELETE support and still
                // reflects a valid subscription-shrinkage scenario.)
                tBatchEnv.executeSql(String.format("DROP TABLE %s", subscriptionTable)).await();
                createSubscriptionTable(subscriptionTable);
                insertSubscription(subscriptionTable, targetA);

                // Second cycle: no new assignments should be emitted; previously-assigned state
                // remains stable (the enumerator does not revoke already-assigned tables).
                runDiscoveryCycle(context);

                assertThat(context.getSplitsAssignmentSequence())
                        .as("Shrinking subscription should not emit new assignments")
                        .hasSize(assignmentsAfterFirst);
                assertThat(assignedTableNames(context)).containsExactlyInAnyOrder(targetA, targetB);
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
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Configuration sourceConfig = buildSourceConfig(flussConfig, pattern);
        return new FlussSourceEnumerator(
                context,
                discoverer,
                flussConfig,
                sourceConfig,
                OffsetsInitializer.earliest(),
                DISCOVERY_INTERVAL_MS,
                new HashSet<>());
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
            return java.util.Collections.emptySet();
        }
        SplitsAssignment<FlussSplitBase> last = sequence.get(sequence.size() - 1);
        return last.assignment().values().stream()
                .flatMap(List::stream)
                .map(split -> split.getPhysicalTablePath().getTableName())
                .collect(Collectors.toSet());
    }

    private void createPkTable(String tableName) throws Exception {
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();
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
        return java.util.regex.Pattern.quote(database) + "\\." + tablePattern;
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
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        return conf;
    }
}
