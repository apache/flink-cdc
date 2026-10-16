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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkOptions;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;
import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** Validated configuration for Iceberg's table maintenance topology. */
@Internal
public final class MaintenanceOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String PREFIX = "sink.maintenance.";
    public static final String JDBC_PROPERTIES_PREFIX = PREFIX + "lock.jdbc.properties.";
    public static final ConfigOption<Boolean> ENABLED =
            key(PREFIX + "enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable Iceberg TableMaintenance for target tables.");
    public static final ConfigOption<String> TABLES =
            key(PREFIX + "tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional semicolon-separated literal target table identifiers. When omitted, discover targets from the source and routing rules at submission. Missing targets are maintained after CDC creates them.");
    public static final ConfigOption<String> UID_PREFIX =
            key(PREFIX + "uid-prefix")
                    .stringType()
                    .defaultValue("iceberg-maintenance")
                    .withDescription(
                            "Stable UID and lock ID prefix, unique to this maintenance pipeline.");
    public static final ConfigOption<Integer> PARALLELISM =
            key(PREFIX + "parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Default parallelism of maintenance tasks for each table.");
    public static final ConfigOption<String> SLOT_SHARING_GROUP =
            key(PREFIX + "slot-sharing-group")
                    .stringType()
                    .defaultValue("iceberg-maintenance")
                    .withDescription("Slot sharing group for maintenance operators.");
    public static final ConfigOption<Duration> RATE_LIMIT =
            key(PREFIX + "rate-limit")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "Minimum trigger interval and table polling interval, at least one second.");
    public static final ConfigOption<Duration> LOCK_CHECK_DELAY =
            key(PREFIX + "lock-check-delay")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Delay before retrying acquisition of the maintenance lock.");
    public static final ConfigOption<Integer> MAX_READ_BACK =
            key(PREFIX + "max-read-back")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum snapshots read back by the Iceberg monitor per poll.");
    public static final ConfigOption<String> JDBC_URI =
            key(PREFIX + "lock.jdbc.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JDBC URL for the maintenance lock database. Requires a JDBC driver at runtime.");
    public static final ConfigOption<Boolean> JDBC_INIT_LOCK_TABLES =
            key(PREFIX + "lock.jdbc.init-lock-tables")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Create the Iceberg maintenance lock table if it does not exist.");
    public static final ConfigOption<Boolean> REWRITE_ENABLED =
            key(PREFIX + "rewrite-data-files.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable data file rewriting.");
    public static final ConfigOption<Duration> REWRITE_INTERVAL =
            key(PREFIX + "rewrite-data-files.interval")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("Time interval for data file rewriting.");
    public static final ConfigOption<Integer> REWRITE_COMMIT_COUNT =
            key(PREFIX + "rewrite-data-files.commit-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional additional commit-count trigger for data file rewriting (OR with interval).");
    public static final ConfigOption<Integer> REWRITE_DATA_FILE_COUNT =
            key(PREFIX + "rewrite-data-files.data-file-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional additional new-data-file-count trigger (OR with other triggers).");
    public static final ConfigOption<Long> REWRITE_TARGET_FILE_SIZE =
            key(PREFIX + "rewrite-data-files.target-file-size-bytes")
                    .longType()
                    .defaultValue(536870912L)
                    .withDescription("Target size of rewritten data files in bytes.");
    public static final ConfigOption<Integer> REWRITE_MIN_INPUT_FILES =
            key(PREFIX + "rewrite-data-files.min-input-files")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Minimum input file count used by the rewrite planner.");
    public static final ConfigOption<Integer> REWRITE_DELETE_FILE_THRESHOLD =
            key(PREFIX + "rewrite-data-files.delete-file-threshold")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "Delete file count associated with a data file that makes it eligible for rewriting.");
    public static final ConfigOption<Long> REWRITE_MAX_BYTES =
            key(PREFIX + "rewrite-data-files.max-rewrite-bytes")
                    .longType()
                    .defaultValue(10737418240L)
                    .withDescription("Maximum bytes to rewrite per run.");
    public static final ConfigOption<Boolean> EXPIRE_ENABLED =
            key(PREFIX + "expire-snapshots.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable snapshot expiration and cleanup of unreferenced files.");
    public static final ConfigOption<Duration> EXPIRE_INTERVAL =
            key(PREFIX + "expire-snapshots.interval")
                    .durationType()
                    .defaultValue(Duration.ofDays(1))
                    .withDescription("Time interval for snapshot expiration.");
    public static final ConfigOption<Integer> EXPIRE_COMMIT_COUNT =
            key(PREFIX + "expire-snapshots.commit-count")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional additional commit-count trigger for snapshot expiration (OR with interval).");
    public static final ConfigOption<Duration> EXPIRE_MAX_AGE =
            key(PREFIX + "expire-snapshots.max-age")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription(
                            "Age threshold for expiring snapshots. Must cover recovery and reader retention needs.");
    public static final ConfigOption<Integer> EXPIRE_RETAIN_LAST =
            key(PREFIX + "expire-snapshots.retain-last")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Minimum number of snapshots to retain.");
    public static final ConfigOption<Boolean> ORPHAN_ENABLED =
            key(PREFIX + "delete-orphan-files.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable deletion of unreferenced files under each configured table location.");
    public static final ConfigOption<Duration> ORPHAN_INTERVAL =
            key(PREFIX + "delete-orphan-files.interval")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription("Time interval for orphan file deletion.");
    public static final ConfigOption<Duration> ORPHAN_MIN_AGE =
            key(PREFIX + "delete-orphan-files.min-age")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription(
                            "Minimum orphan file age, at least three days and longer than pending writes and recovery.");
    public static final ConfigOption<Integer> DELETE_BATCH_SIZE =
            key(PREFIX + "delete-batch-size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "File deletion batch size for snapshot expiration and orphan cleanup.");

    private final Configuration configuration;
    private final List<TableId> tables;

    private MaintenanceOptions(Configuration configuration, List<TableId> tables) {
        this.configuration = configuration;
        this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
    }

    public static MaintenanceOptions disabled() {
        return new MaintenanceOptions(new Configuration(), Collections.emptyList());
    }

    public static MaintenanceOptions fromConfiguration(Configuration source) {
        Configuration conf = Configuration.fromMap(source.toMap());
        if (!conf.get(ENABLED)) {
            return disabled();
        }
        checkArgument(
                !conf.get(IcebergDataSinkOptions.SINK_COMPACTION_ENABLED),
                "sink.compaction.enabled and sink.maintenance.enabled cannot both be true.");
        checkArgument(
                conf.get(REWRITE_ENABLED) || conf.get(EXPIRE_ENABLED) || conf.get(ORPHAN_ENABLED),
                "sink.maintenance.enabled requires at least one enabled maintenance task.");
        if (conf.contains(TABLES)) {
            requireText(conf, TABLES);
        }
        requireText(conf, UID_PREFIX);
        requireText(conf, SLOT_SHARING_GROUP);
        requireText(conf, JDBC_URI);
        checkArgument(
                conf.get(JDBC_URI).startsWith("jdbc:"), "%s must be a JDBC URL.", JDBC_URI.key());
        for (ConfigOption<Integer> option :
                Arrays.asList(PARALLELISM, MAX_READ_BACK, DELETE_BATCH_SIZE)) {
            positive(conf, option);
        }
        positiveDuration(conf, RATE_LIMIT);
        checkArgument(
                conf.get(RATE_LIMIT).toMillis() >= 1000
                        && conf.get(RATE_LIMIT).toMillis() % 1000 == 0,
                "%s must be a whole number of seconds, at least 1 s.",
                RATE_LIMIT.key());
        positiveDuration(conf, LOCK_CHECK_DELAY);
        if (conf.get(REWRITE_ENABLED)) {
            positiveDuration(conf, REWRITE_INTERVAL);
            for (ConfigOption<? extends Number> option :
                    Arrays.asList(
                            REWRITE_TARGET_FILE_SIZE,
                            REWRITE_MIN_INPUT_FILES,
                            REWRITE_DELETE_FILE_THRESHOLD,
                            REWRITE_MAX_BYTES)) {
                positive(conf, option);
            }
            optionalPositive(conf, REWRITE_COMMIT_COUNT);
            optionalPositive(conf, REWRITE_DATA_FILE_COUNT);
        }
        if (conf.get(EXPIRE_ENABLED)) {
            positiveDuration(conf, EXPIRE_INTERVAL);
            positiveDuration(conf, EXPIRE_MAX_AGE);
            positive(conf, EXPIRE_RETAIN_LAST);
            optionalPositive(conf, EXPIRE_COMMIT_COUNT);
        }
        if (conf.get(ORPHAN_ENABLED)) {
            positiveDuration(conf, ORPHAN_INTERVAL);
            positiveDuration(conf, ORPHAN_MIN_AGE);
            checkArgument(
                    conf.get(ORPHAN_MIN_AGE).compareTo(Duration.ofDays(3)) >= 0,
                    "%s must be at least 3 d.",
                    ORPHAN_MIN_AGE.key());
        }
        if (!conf.contains(TABLES)) {
            return new MaintenanceOptions(conf, Collections.emptyList());
        }
        Set<String> identifiers = new TreeSet<>();
        for (String entry : conf.get(TABLES).split(";", -1)) {
            String identifier = entry.trim();
            checkArgument(
                    !identifier.isEmpty(), "%s contains an empty table identifier.", TABLES.key());
            String[] parts = identifier.split("\\.", -1);
            checkArgument(
                    parts.length >= 2 && parts.length <= 3,
                    "%s requires literal database.table or namespace.database.table identifiers: %s",
                    TABLES.key(),
                    identifier);
            for (String part : parts) {
                checkArgument(
                        part.matches("[A-Za-z0-9_\\-]+"),
                        "%s requires unquoted literal identifiers, not patterns: %s",
                        TABLES.key(),
                        identifier);
            }
            checkArgument(
                    identifiers.add(identifier),
                    "%s contains duplicate table %s.",
                    TABLES.key(),
                    identifier);
        }
        List<TableId> tables = new ArrayList<>();
        identifiers.forEach(identifier -> tables.add(TableId.parse(identifier)));
        return new MaintenanceOptions(conf, tables);
    }

    public boolean requiresTableDiscovery() {
        return get(ENABLED) && tables.isEmpty() && !configuration.contains(TABLES);
    }

    /** Validates and normalizes targets from any discovery provider before building maintenance. */
    public MaintenanceOptions withDiscoveredTables(List<TableId> discoveredTables) {
        checkArgument(
                requiresTableDiscovery(), "Maintenance target tables are already configured.");
        checkArgument(
                !discoveredTables.isEmpty(),
                "No maintenance target tables were discovered from the source and routing rules. "
                        + "Configure sink.maintenance.tables explicitly to select targets.");
        Set<TableId> identifiers = new HashSet<>();
        for (TableId table : discoveredTables) {
            checkArgument(
                    TableId.parse(table.identifier()).equals(table),
                    "Cannot represent discovered maintenance target %s as a literal identifier.",
                    table);
            identifiers.add(table);
        }
        List<TableId> resolved = new ArrayList<>(identifiers);
        resolved.sort(Comparator.comparing(TableId::identifier));
        return new MaintenanceOptions(configuration, resolved);
    }

    public <T> T get(ConfigOption<T> option) {
        return configuration.get(option);
    }

    public List<TableId> tables() {
        return tables;
    }

    public Map<String, String> jdbcProperties() {
        Map<String, String> properties = new HashMap<>();
        configuration
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(JDBC_PROPERTIES_PREFIX)) {
                                properties.put(
                                        "jdbc." + key.substring(JDBC_PROPERTIES_PREFIX.length()),
                                        value);
                            }
                        });
        properties.put(
                "flink-maintenance.lock.jdbc.init-lock-tables",
                get(JDBC_INIT_LOCK_TABLES).toString());
        return properties;
    }

    private static void requireText(Configuration conf, ConfigOption<String> option) {
        String value = conf.get(option);
        checkArgument(
                value != null && !value.trim().isEmpty(), "%s must be configured.", option.key());
    }

    private static void positive(Configuration conf, ConfigOption<? extends Number> option) {
        checkArgument(conf.get(option).longValue() > 0, "%s must be positive.", option.key());
    }

    private static void optionalPositive(Configuration conf, ConfigOption<Integer> option) {
        if (conf.contains(option)) {
            positive(conf, option);
        }
    }

    private static void positiveDuration(Configuration conf, ConfigOption<Duration> option) {
        checkArgument(conf.get(option).toMillis() > 0, "%s must be at least 1 ms.", option.key());
    }

    public static Set<ConfigOption<?>> supportedOptions() {
        return new HashSet<>(
                Arrays.asList(
                        ENABLED,
                        TABLES,
                        UID_PREFIX,
                        PARALLELISM,
                        SLOT_SHARING_GROUP,
                        RATE_LIMIT,
                        LOCK_CHECK_DELAY,
                        MAX_READ_BACK,
                        JDBC_URI,
                        JDBC_INIT_LOCK_TABLES,
                        REWRITE_ENABLED,
                        REWRITE_INTERVAL,
                        REWRITE_COMMIT_COUNT,
                        REWRITE_DATA_FILE_COUNT,
                        REWRITE_TARGET_FILE_SIZE,
                        REWRITE_MIN_INPUT_FILES,
                        REWRITE_DELETE_FILE_THRESHOLD,
                        REWRITE_MAX_BYTES,
                        EXPIRE_ENABLED,
                        EXPIRE_INTERVAL,
                        EXPIRE_COMMIT_COUNT,
                        EXPIRE_MAX_AGE,
                        EXPIRE_RETAIN_LAST,
                        ORPHAN_ENABLED,
                        ORPHAN_INTERVAL,
                        ORPHAN_MIN_AGE,
                        DELETE_BATCH_SIZE));
    }
}
