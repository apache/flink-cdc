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

package org.apache.flink.cdc.connectors.dws.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSink;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkConfig;
import org.apache.flink.configuration.MemorySize;

import com.huaweicloud.dws.client.model.WriteMode;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.AUTO_BATCH_FLUSH_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.AUTO_FLUSH_MAX_INTERVAL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CASE_SENSITIVE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_IDLE_MS;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_USE_COUNT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_USE_TIME_SECONDS;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_USE_TIME_THRESHOLD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_MONITOR_PERIOD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_NAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_TIMEOUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_SOCKET_TIMEOUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_TIME_OUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DISTRIBUTION_KEY;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DRIVER;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_BUFFER_ALL_MAX_BYTES;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_BUFFER_PARTITION_MAX_BYTES;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_BUFFER_TABLE_MAX_BYTES;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_JDBC_MAX_IDLE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_JDBC_MAX_USE_TIME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_RETRY_SLEEP_BASE_TIME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_RETRY_SLEEP_RANDOM_TIME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_TIMEOUT_STATEMENT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_TIMEOUT_TASK;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_WRITE_FORCE_FLUSH_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_WRITE_PARTITION_MAX;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_WRITE_PARTITION_MIN;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_WRITE_PARTITION_POLICY;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_WRITE_THREAD_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DWS_CLIENT_WRITE_USE_COPY_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.ENABLE_AUTO_FLUSH;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.ENABLE_DN_PARTITION;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.LOG_SWITCH;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.NEED_CONNECTION_POOL_MONITOR;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SCHEMA;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_PARALLELISM;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.TABLE_NAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.URL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.WRITE_MODE;

/** A {@link DataSinkFactory} to create {@link DwsDataSink}. */
@Internal
public class DwsDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "dws";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();

        Configuration config = context.getFactoryConfiguration();
        ZoneId zoneId = resolveZoneId(config, context.getPipelineConfiguration());
        validateUnsupportedOptions(config);
        validateDistributionOptions(config);
        validateAliases(config);

        DwsDataSinkConfig sinkConfig =
                DwsDataSinkConfig.builder()
                        .withUrl(resolveJdbcUrl(config))
                        .withUsername(config.get(USERNAME))
                        .withPassword(config.get(PASSWORD))
                        .withZoneId(zoneId)
                        .withCaseSensitive(config.get(CASE_SENSITIVE))
                        .withDefaultSchema(config.get(SCHEMA))
                        .withEnableDelete(config.get(SINK_ENABLE_DELETE))
                        .withEnableDnPartition(config.get(ENABLE_DN_PARTITION))
                        .withDistributionKey(config.getOptional(DISTRIBUTION_KEY).orElse(null))
                        .withWriteMode(parseWriteMode(config.get(WRITE_MODE)))
                        .withEnableAutoFlush(config.get(ENABLE_AUTO_FLUSH))
                        .withAutoFlushBatchSize(config.get(AUTO_BATCH_FLUSH_SIZE))
                        .withAutoFlushMaxInterval(config.get(AUTO_FLUSH_MAX_INTERVAL))
                        .withWriteThreadSize(config.get(DWS_CLIENT_WRITE_THREAD_SIZE))
                        .withUseCopyBatchSize(config.get(DWS_CLIENT_WRITE_USE_COPY_SIZE))
                        .withForceFlushBatchSize(config.get(DWS_CLIENT_WRITE_FORCE_FLUSH_SIZE))
                        .withRetryMaxTimes(config.get(SINK_MAX_RETRIES))
                        .withRetrySleepBaseTime(config.get(DWS_CLIENT_RETRY_SLEEP_BASE_TIME))
                        .withRetrySleepRandomTime(config.get(DWS_CLIENT_RETRY_SLEEP_RANDOM_TIME))
                        .withTaskTimeout(config.get(DWS_CLIENT_TIMEOUT_TASK))
                        .withStatementTimeout(config.get(DWS_CLIENT_TIMEOUT_STATEMENT))
                        .withConnectionMaxUseTime(resolveConnectionMaxUseTime(config))
                        .withConnectionMaxIdle(resolveConnectionMaxIdle(config))
                        .withBufferAllMaxBytes(parseBytes(config, DWS_CLIENT_BUFFER_ALL_MAX_BYTES))
                        .withBufferTableMaxBytes(
                                parseBytes(config, DWS_CLIENT_BUFFER_TABLE_MAX_BYTES))
                        .withBufferPartitionMaxBytes(
                                parseBytes(config, DWS_CLIENT_BUFFER_PARTITION_MAX_BYTES))
                        .build();
        return new DwsDataSink(sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(AUTO_BATCH_FLUSH_SIZE);
        optionalOptions.add(AUTO_FLUSH_MAX_INTERVAL);
        optionalOptions.add(LOG_SWITCH);
        optionalOptions.add(TABLE_NAME);
        optionalOptions.add(DRIVER);
        optionalOptions.add(PIPELINE_LOCAL_TIME_ZONE);
        optionalOptions.add(CONNECTION_SIZE);
        optionalOptions.add(SCHEMA);
        optionalOptions.add(ENABLE_AUTO_FLUSH);
        optionalOptions.add(ENABLE_DN_PARTITION);
        optionalOptions.add(DWS_CLIENT_WRITE_THREAD_SIZE);
        optionalOptions.add(DWS_CLIENT_WRITE_USE_COPY_SIZE);
        optionalOptions.add(DWS_CLIENT_WRITE_FORCE_FLUSH_SIZE);
        optionalOptions.add(DWS_CLIENT_RETRY_SLEEP_BASE_TIME);
        optionalOptions.add(DWS_CLIENT_RETRY_SLEEP_RANDOM_TIME);
        optionalOptions.add(DWS_CLIENT_TIMEOUT_TASK);
        optionalOptions.add(DWS_CLIENT_TIMEOUT_STATEMENT);
        optionalOptions.add(DWS_CLIENT_BUFFER_ALL_MAX_BYTES);
        optionalOptions.add(DWS_CLIENT_BUFFER_TABLE_MAX_BYTES);
        optionalOptions.add(DWS_CLIENT_BUFFER_PARTITION_MAX_BYTES);
        optionalOptions.add(DWS_CLIENT_WRITE_PARTITION_POLICY);
        optionalOptions.add(DWS_CLIENT_WRITE_PARTITION_MIN);
        optionalOptions.add(DWS_CLIENT_WRITE_PARTITION_MAX);
        optionalOptions.add(DISTRIBUTION_KEY);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_ENABLE_DELETE);
        optionalOptions.add(CONNECTION_MAX_USE_TIME_SECONDS);
        optionalOptions.add(CONNECTION_MAX_USE_TIME_THRESHOLD);
        optionalOptions.add(DWS_CLIENT_JDBC_MAX_USE_TIME);
        optionalOptions.add(CONNECTION_MAX_IDLE_MS);
        optionalOptions.add(DWS_CLIENT_JDBC_MAX_IDLE);
        optionalOptions.add(CONNECTION_TIME_OUT);
        optionalOptions.add(CONNECTION_POOL_NAME);
        optionalOptions.add(CONNECTION_POOL_SIZE);
        optionalOptions.add(CONNECTION_POOL_TIMEOUT);
        optionalOptions.add(CONNECTION_SOCKET_TIMEOUT);
        optionalOptions.add(CONNECTION_MAX_USE_COUNT);
        optionalOptions.add(NEED_CONNECTION_POOL_MONITOR);
        optionalOptions.add(CONNECTION_POOL_MONITOR_PERIOD);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(CASE_SENSITIVE);
        optionalOptions.add(WRITE_MODE);
        return optionalOptions;
    }

    private static ZoneId resolveZoneId(
            Configuration factoryConfiguration, Configuration pipelineConfiguration) {
        String zone =
                factoryConfiguration.contains(PIPELINE_LOCAL_TIME_ZONE)
                        ? factoryConfiguration.get(PIPELINE_LOCAL_TIME_ZONE)
                        : pipelineConfiguration.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE);
        return PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zone)
                ? ZoneId.systemDefault()
                : ZoneId.of(zone);
    }

    private static WriteMode parseWriteMode(String writeMode) {
        if (writeMode == null) {
            return WriteMode.AUTO;
        }

        try {
            WriteMode parsed = WriteMode.valueOf(writeMode.toUpperCase(Locale.ROOT));
            if (parsed == WriteMode.AUTO
                    || parsed == WriteMode.UPSERT
                    || parsed == WriteMode.COPY_UPSERT
                    || parsed == WriteMode.COPY_MERGE) {
                return parsed;
            }
        } catch (IllegalArgumentException e) {
            throw unsupportedWriteMode(writeMode, e);
        }
        throw unsupportedWriteMode(writeMode, null);
    }

    private static IllegalArgumentException unsupportedWriteMode(
            String writeMode, IllegalArgumentException cause) {
        String message =
                String.format(
                        "Unsupported write-mode '%s'. Supported values: auto, upsert, copy_upsert, copy_merge",
                        writeMode);
        return cause == null
                ? new IllegalArgumentException(message)
                : new IllegalArgumentException(message, cause);
    }

    private static void validateUnsupportedOptions(Configuration config) {
        rejectIfPresent(config, TABLE_NAME, "Use Pipeline route rules to select target tables.");
        rejectIfPresent(
                config, SINK_PARALLELISM, "Use pipeline.parallelism for the keyed sink topology.");
        rejectIfPresent(
                config,
                CONNECTION_SIZE,
                "Use dws.client.write.thread-size; connectionSize is not a client thread count.");
        rejectIfPresent(config, CONNECTION_POOL_NAME, "This is a BINLOG pool option.");
        rejectIfPresent(config, CONNECTION_POOL_SIZE, "This is a BINLOG pool option.");
        rejectIfPresent(config, CONNECTION_POOL_TIMEOUT, "This is a BINLOG pool option.");
        rejectIfPresent(config, CONNECTION_MAX_USE_COUNT, "This is a BINLOG pool option.");
        rejectIfPresent(config, NEED_CONNECTION_POOL_MONITOR, "This is a BINLOG pool option.");
        rejectIfPresent(config, CONNECTION_POOL_MONITOR_PERIOD, "This is a BINLOG pool option.");
        rejectIfPresent(
                config,
                DWS_CLIENT_WRITE_PARTITION_POLICY,
                "Native partitioning is fixed to DYNAMIC with one partition per table.");
        rejectIfPresent(
                config,
                DWS_CLIENT_WRITE_PARTITION_MIN,
                "Native partitioning is fixed to one; use pipeline.parallelism for scaling.");
        rejectIfPresent(
                config,
                DWS_CLIENT_WRITE_PARTITION_MAX,
                "Native partitioning is fixed to one; use pipeline.parallelism for scaling.");

        if (config.contains(LOG_SWITCH) && config.get(LOG_SWITCH)) {
            throw new IllegalArgumentException(
                    "logSwitch=true is not supported because native detailed logs may expose row data or credentials.");
        }
        if (config.contains(DRIVER) && !DRIVER.defaultValue().equals(config.get(DRIVER))) {
            throw new IllegalArgumentException(
                    "Only the GaussDB DWS JDBC driver is supported: " + DRIVER.defaultValue());
        }
    }

    private static void validateDistributionOptions(Configuration config) {
        boolean enabled = config.get(ENABLE_DN_PARTITION);
        String key = config.getOptional(DISTRIBUTION_KEY).orElse(null);
        if (enabled && (key == null || key.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "distribution-key is required when enable-dn-partition=true.");
        }
        if (!enabled && key != null) {
            throw new IllegalArgumentException(
                    "distribution-key requires enable-dn-partition=true.");
        }
    }

    private static void validateAliases(Configuration config) {
        validateAliasConflict(config, WRITE_MODE, "dws.client.write.mode");
        validateAliasConflict(config, AUTO_BATCH_FLUSH_SIZE, "dws.client.write.auto-flush-size");
        validateAliasConflict(
                config, AUTO_FLUSH_MAX_INTERVAL, "dws.client.write.auto-flush-max-interval");
        validateAliasConflict(config, SINK_MAX_RETRIES, "dws.client.retry.max-times");
    }

    private static <T> void validateAliasConflict(
            Configuration config, ConfigOption<T> primary, String alias) {
        Map<String, String> values = config.toMap();
        if (!values.containsKey(primary.key()) || !values.containsKey(alias)) {
            return;
        }
        T primaryValue = parseOptionValue(primary, values.get(primary.key()));
        T aliasValue = parseOptionValue(primary, values.get(alias));
        boolean equal;
        if (primary == WRITE_MODE) {
            equal =
                    parseWriteMode(String.valueOf(primaryValue))
                            == parseWriteMode(String.valueOf(aliasValue));
        } else {
            equal = Objects.equals(primaryValue, aliasValue);
        }
        if (!equal) {
            throw new IllegalArgumentException(
                    String.format(
                            "Conflicting values for options '%s' and '%s'.", primary.key(), alias));
        }
    }

    private static <T> T parseOptionValue(ConfigOption<T> option, String value) {
        return Configuration.fromMap(java.util.Collections.singletonMap(option.key(), value))
                .get(option);
    }

    private static Duration resolveConnectionMaxUseTime(Configuration config) {
        Map<String, String> values = config.toMap();
        Duration selected = null;
        if (values.containsKey(CONNECTION_MAX_USE_TIME_SECONDS.key())) {
            selected = Duration.ofSeconds(config.get(CONNECTION_MAX_USE_TIME_SECONDS));
        }
        if (values.containsKey(CONNECTION_MAX_USE_TIME_THRESHOLD.key())) {
            selected =
                    mergeEquivalentDuration(
                            selected,
                            Duration.ofSeconds(config.get(CONNECTION_MAX_USE_TIME_THRESHOLD)),
                            CONNECTION_MAX_USE_TIME_SECONDS.key(),
                            CONNECTION_MAX_USE_TIME_THRESHOLD.key());
        }
        if (values.containsKey(DWS_CLIENT_JDBC_MAX_USE_TIME.key())) {
            selected =
                    mergeEquivalentDuration(
                            selected,
                            config.get(DWS_CLIENT_JDBC_MAX_USE_TIME),
                            CONNECTION_MAX_USE_TIME_SECONDS.key(),
                            DWS_CLIENT_JDBC_MAX_USE_TIME.key());
        }
        return selected == null ? Duration.ofHours(1) : selected;
    }

    private static Duration resolveConnectionMaxIdle(Configuration config) {
        Map<String, String> values = config.toMap();
        Duration selected = null;
        if (values.containsKey(CONNECTION_MAX_IDLE_MS.key())) {
            selected = Duration.ofMillis(config.get(CONNECTION_MAX_IDLE_MS));
        }
        if (values.containsKey(DWS_CLIENT_JDBC_MAX_IDLE.key())) {
            selected =
                    mergeEquivalentDuration(
                            selected,
                            config.get(DWS_CLIENT_JDBC_MAX_IDLE),
                            CONNECTION_MAX_IDLE_MS.key(),
                            DWS_CLIENT_JDBC_MAX_IDLE.key());
        }
        return selected == null ? Duration.ofMinutes(1) : selected;
    }

    private static Duration mergeEquivalentDuration(
            Duration current, Duration candidate, String currentKey, String candidateKey) {
        if (current != null && !current.equals(candidate)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Conflicting duration values for options '%s' and '%s'.",
                            currentKey, candidateKey));
        }
        return candidate;
    }

    private static String resolveJdbcUrl(Configuration config) {
        String rawUrl = config.get(URL);
        if (!rawUrl.startsWith("jdbc:gaussdb://")) {
            throw new IllegalArgumentException("jdbc-url must use the jdbc:gaussdb:// protocol.");
        }
        int queryStart = rawUrl.indexOf('?');
        String base = queryStart < 0 ? rawUrl : rawUrl.substring(0, queryStart);
        String rawQuery = queryStart < 0 ? "" : rawUrl.substring(queryStart + 1);
        LinkedHashMap<String, String> query = new LinkedHashMap<>();
        if (!rawQuery.isEmpty()) {
            for (String pair : rawQuery.split("&")) {
                int separator = pair.indexOf('=');
                String key = separator < 0 ? pair : pair.substring(0, separator);
                String value = separator < 0 ? "" : pair.substring(separator + 1);
                if (key.isEmpty() || query.putIfAbsent(key, value) != null) {
                    throw new IllegalArgumentException(
                            "jdbc-url contains an empty or duplicate query parameter.");
                }
            }
        }
        query.put(
                "connectTimeout",
                Long.toString(
                        resolveUrlTimeoutSeconds(
                                config, query, "connectTimeout", CONNECTION_TIME_OUT, 10L)));
        query.put(
                "socketTimeout",
                Long.toString(
                        resolveUrlTimeoutSeconds(
                                config, query, "socketTimeout", CONNECTION_SOCKET_TIMEOUT, 60L)));
        return base
                + "?"
                + query.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining("&"));
    }

    private static long resolveUrlTimeoutSeconds(
            Configuration config,
            Map<String, String> query,
            String queryKey,
            ConfigOption<? extends Number> legacyOption,
            long defaultSeconds) {
        Long querySeconds = null;
        if (query.containsKey(queryKey)) {
            try {
                querySeconds = Long.parseLong(query.get(queryKey));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "jdbc-url parameter " + queryKey + " must be a positive number of seconds.",
                        e);
            }
            if (querySeconds <= 0) {
                throw new IllegalArgumentException(
                        "jdbc-url parameter " + queryKey + " must be positive.");
            }
        }
        Long legacySeconds = null;
        if (config.toMap().containsKey(legacyOption.key())) {
            long milliseconds = config.get(legacyOption).longValue();
            if (milliseconds <= 0 || milliseconds % 1000L != 0) {
                throw new IllegalArgumentException(
                        legacyOption.key()
                                + " must be positive and divisible by 1000 milliseconds.");
            }
            legacySeconds = milliseconds / 1000L;
        }
        if (querySeconds != null && legacySeconds != null && !querySeconds.equals(legacySeconds)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Conflicting timeout values for jdbc-url parameter '%s' and option '%s'.",
                            queryKey, legacyOption.key()));
        }
        return querySeconds != null
                ? querySeconds
                : legacySeconds != null ? legacySeconds : defaultSeconds;
    }

    private static long parseBytes(Configuration config, ConfigOption<String> option) {
        try {
            String value =
                    config.get(option)
                            .trim()
                            .replaceFirst("(?i)kib$", "kibibytes")
                            .replaceFirst("(?i)mib$", "mebibytes")
                            .replaceFirst("(?i)gib$", "gibibytes")
                            .replaceFirst("(?i)tib$", "tebibytes");
            long bytes = MemorySize.parse(value).getBytes();
            if (bytes <= 0) {
                throw new IllegalArgumentException(option.key() + " must be positive.");
            }
            return bytes;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Invalid byte size for option '" + option.key() + "'.", e);
        }
    }

    private static void rejectIfPresent(
            Configuration config, ConfigOption<?> option, String replacement) {
        if (config.contains(option)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' is not supported by the native DWS writer. %s",
                            option.key(), replacement));
        }
    }
}
