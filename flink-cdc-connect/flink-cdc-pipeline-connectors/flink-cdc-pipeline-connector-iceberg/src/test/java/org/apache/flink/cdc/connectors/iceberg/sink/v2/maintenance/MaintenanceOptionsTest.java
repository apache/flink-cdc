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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSink;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaintenanceOptionsTest {
    @Test
    void discoversTargetsWhenOmittedAndSerializesResolvedOptions() throws Exception {
        Map<String, String> conf = validOptions();
        conf.remove("sink.maintenance.tables");
        IcebergDataSink sink = create(conf);
        assertThat(sink.getMaintenanceOptions().requiresTableDiscovery()).isTrue();
        sink.discoverTargetTables(
                () ->
                        Arrays.asList(
                                TableId.parse("sales.users"),
                                TableId.parse("sales.orders"),
                                TableId.parse("sales.orders")));
        MaintenanceOptions restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink.getMaintenanceOptions()),
                        getClass().getClassLoader());
        assertThat(restored.requiresTableDiscovery()).isFalse();
        assertThat(restored.tables())
                .containsExactly(TableId.parse("sales.orders"), TableId.parse("sales.users"));
        sink.discoverTargetTables(
                () -> {
                    throw new AssertionError("Already discovered");
                });
    }

    @Test
    void explicitTargetsAndDisabledMaintenanceDoNotDiscoverSources() {
        create(validOptions())
                .discoverTargetTables(
                        () -> {
                            throw new AssertionError("Explicit targets");
                        });
        create(new HashMap<>())
                .discoverTargetTables(
                        () -> {
                            throw new AssertionError("Disabled");
                        });
    }

    @Test
    void preservesDiscoveredLiteralNamesWithoutParsingThemAsConfiguration() throws Exception {
        Map<String, String> conf = validOptions();
        conf.remove("sink.maintenance.tables");
        IcebergDataSink sink = create(conf);
        java.util.List<TableId> tables =
                Arrays.asList(
                        TableId.tableId("sales", "order$history"),
                        TableId.tableId("sales", "订单"),
                        TableId.tableId("sales", "semi;colon"));
        sink.discoverTargetTables(() -> tables);
        MaintenanceOptions restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(sink.getMaintenanceOptions()),
                        getClass().getClassLoader());
        assertThat(restored.requiresTableDiscovery()).isFalse();
        assertThat(restored.tables()).containsExactlyInAnyOrderElementsOf(tables);
    }

    @Test
    void rejectsEmptyDiscoveryAndAmbiguousIdentifiers() {
        Map<String, String> conf = validOptions();
        conf.remove("sink.maintenance.tables");
        IcebergDataSink sink = create(conf);
        assertThatThrownBy(() -> sink.discoverTargetTables(Collections::emptyList))
                .hasMessageContaining("sink.maintenance.tables")
                .hasRootCauseMessage(
                        "No maintenance target tables were discovered from the source and routing rules. "
                                + "Configure sink.maintenance.tables explicitly to select targets.");
        assertThatThrownBy(
                        () ->
                                sink.discoverTargetTables(
                                        () ->
                                                Collections.singletonList(
                                                        TableId.tableId("db", "orders.v2"))))
                .hasRootCauseMessage(
                        "Cannot represent discovered maintenance target db.orders.v2 as a literal identifier.");
    }

    @Test
    void disabledByDefault() {
        MaintenanceOptions options = create(new HashMap<>()).getMaintenanceOptions();
        assertThat(options.get(MaintenanceOptions.ENABLED)).isFalse();
        assertThat(options.tables()).isEmpty();
    }

    @Test
    void acceptsOptionsThroughFactoryAndSerializesThem() throws Exception {
        Map<String, String> conf = validOptions();
        conf.put("sink.maintenance.tables", "sales.users; sales.orders");
        conf.put("sink.maintenance.rate-limit", "10 s");
        conf.put("sink.maintenance.rewrite-data-files.enabled", "true");
        conf.put("sink.maintenance.rewrite-data-files.commit-count", "20");
        conf.put("sink.maintenance.delete-orphan-files.enabled", "true");
        conf.put("sink.maintenance.lock.jdbc.properties.user", "maintenance-user");
        conf.put("sink.maintenance.lock.jdbc.properties.password", "test-password");
        MaintenanceOptions options = create(conf).getMaintenanceOptions();
        conf.put("sink.maintenance.tables", "other.table");
        MaintenanceOptions restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(options), getClass().getClassLoader());
        assertThat(restored.tables())
                .containsExactly(TableId.parse("sales.orders"), TableId.parse("sales.users"));
        assertThat(restored.get(MaintenanceOptions.RATE_LIMIT)).isEqualTo(Duration.ofSeconds(10));
        assertThat(restored.get(MaintenanceOptions.REWRITE_COMMIT_COUNT)).isEqualTo(20);
        assertThat(restored.jdbcProperties())
                .containsEntry("jdbc.user", "maintenance-user")
                .containsEntry("jdbc.password", "test-password");
        assertThat(restored.get(MaintenanceOptions.ORPHAN_ENABLED)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "sales.*",
                "sales.orders;",
                "sales..orders",
                "orders",
                "sales.orders;sales.orders"
            })
    void rejectsNonLiteralOrDuplicateTargets(String targets) {
        Map<String, String> conf = validOptions();
        conf.put("sink.maintenance.tables", targets);
        assertThatThrownBy(() -> create(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.maintenance.tables");
    }

    @ParameterizedTest
    @CsvSource({
        "parallelism,0",
        "max-read-back,0",
        "delete-batch-size,-1",
        "rate-limit,0 ms",
        "rate-limit,500 ms",
        "rate-limit,1500 ms",
        "lock-check-delay,0 ms",
        "expire-snapshots.interval,0 ms",
        "expire-snapshots.max-age,0 ms",
        "expire-snapshots.retain-last,0",
        "expire-snapshots.commit-count,-1",
        "rewrite-data-files.interval,0 ms",
        "rewrite-data-files.target-file-size-bytes,0",
        "rewrite-data-files.min-input-files,0",
        "rewrite-data-files.max-rewrite-bytes,0",
        "rewrite-data-files.commit-count,0",
        "rewrite-data-files.data-file-count,-1",
        "rewrite-data-files.delete-file-threshold,0",
        "delete-orphan-files.interval,0 ms",
        "delete-orphan-files.min-age,2 d",
        "lock.jdbc.uri,not-a-jdbc-url"
    })
    void rejectsInvalidSettings(String key, String value) {
        Map<String, String> conf = validOptions();
        conf.put("sink.maintenance.rewrite-data-files.enabled", "true");
        conf.put("sink.maintenance.delete-orphan-files.enabled", "true");
        conf.put("sink.maintenance." + key, value);
        assertThatThrownBy(() -> create(conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.maintenance." + key);
    }

    @Test
    void requiresLockAndTaskAndRejectsLegacyCompaction() {
        Map<String, String> conf = validOptions();
        conf.remove("sink.maintenance.lock.jdbc.uri");
        Map<String, String> withoutLock = conf;
        assertThatThrownBy(() -> create(withoutLock)).hasMessageContaining("lock.jdbc.uri");
        conf = validOptions();
        conf.put("sink.maintenance.expire-snapshots.enabled", "false");
        Map<String, String> withoutTask = conf;
        assertThatThrownBy(() -> create(withoutTask)).hasMessageContaining("at least one");
        conf = validOptions();
        conf.put("sink.compaction.enabled", "true");
        Map<String, String> conflicting = conf;
        assertThatThrownBy(() -> create(conflicting)).hasMessageContaining("cannot both be true");
    }

    @Test
    void rejectsUnknownMaintenanceOption() {
        Map<String, String> conf = validOptions();
        conf.put("sink.maintenance.expire-snapshots.retain-lsat", "10");
        assertThatThrownBy(() -> create(conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("retain-lsat");
    }

    static Map<String, String> validOptions() {
        Map<String, String> conf = new HashMap<>();
        conf.put("sink.maintenance.enabled", "true");
        conf.put("sink.maintenance.tables", "sales.orders");
        conf.put("sink.maintenance.expire-snapshots.enabled", "true");
        conf.put("sink.maintenance.lock.jdbc.uri", "jdbc:derby:memory:maintenance;create=true");
        return conf;
    }

    private static IcebergDataSink create(Map<String, String> values) {
        Configuration conf = Configuration.fromMap(values);
        return (IcebergDataSink)
                new IcebergDataSinkFactory()
                        .createDataSink(
                                new FactoryHelper.DefaultContext(
                                        conf,
                                        new Configuration(),
                                        MaintenanceOptionsTest.class.getClassLoader()));
    }
}
