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

package org.apache.flink.cdc.connectors.lancedb.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSink;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkConfig;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbPathUtils;
import org.apache.flink.table.api.ValidationException;

import com.lancedb.lance.WriteParams;

import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.CREATE_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.ROOT_PATH;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SCHEMA_EVOLUTION_ENABLED;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SCHEMA_VALIDATION_ENABLED;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SINK_BATCH_MAX_ROWS_PER_COMMIT;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SINK_CHANGELOG_MODE;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SINK_FLUSH_MAX_ROWS;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.SINK_RETRY_BACKOFF;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.STORAGE_OPTIONS_PREFIX;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.TABLE_NAME_NORMALIZE_ENABLED;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.TABLE_PATH_MAPPING;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.TIMEZONE;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.WRITE_ENABLE_STABLE_ROW_IDS;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.WRITE_MAX_BYTES_PER_FILE;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.WRITE_MAX_ROWS_PER_FILE;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.WRITE_MAX_ROWS_PER_GROUP;
import static org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkOptions.WRITE_MODE;

/** Factory for creating configured {@link LanceDbDataSink} instances. */
@Internal
public class LanceDbDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "lancedb";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validateExcept(STORAGE_OPTIONS_PREFIX);

        Configuration config = context.getFactoryConfiguration();
        validateNonBlank(config.get(ROOT_PATH), ROOT_PATH.key());
        validatePositive(config.get(SINK_FLUSH_MAX_ROWS), SINK_FLUSH_MAX_ROWS.key());
        validatePositive(
                config.get(SINK_BATCH_MAX_ROWS_PER_COMMIT), SINK_BATCH_MAX_ROWS_PER_COMMIT.key());
        validatePositive(config.get(SINK_FLUSH_INTERVAL).toMillis(), SINK_FLUSH_INTERVAL.key());
        validateNonNegative(config.get(SINK_MAX_RETRIES), SINK_MAX_RETRIES.key());
        validateNonNegative(config.get(SINK_RETRY_BACKOFF).toMillis(), SINK_RETRY_BACKOFF.key());
        validatePositive(config.get(WRITE_MAX_ROWS_PER_FILE), WRITE_MAX_ROWS_PER_FILE.key());
        validatePositive(config.get(WRITE_MAX_ROWS_PER_GROUP), WRITE_MAX_ROWS_PER_GROUP.key());
        validatePositive(config.get(WRITE_MAX_BYTES_PER_FILE), WRITE_MAX_BYTES_PER_FILE.key());
        String changelogMode = normalizeChangelogMode(config.get(SINK_CHANGELOG_MODE));
        validateWriteMode(config.get(WRITE_MODE));
        ZoneId zoneId = parseZoneId(config.get(TIMEZONE));

        LanceDbDataSinkConfig sinkConfig =
                new LanceDbDataSinkConfig(
                        LanceDbPathUtils.trimTrailingSlash(config.get(ROOT_PATH).trim()),
                        parseTablePathMapping(config.get(TABLE_PATH_MAPPING)),
                        config.get(TABLE_NAME_NORMALIZE_ENABLED),
                        config.get(CREATE_TABLE_ENABLED),
                        config.get(SCHEMA_VALIDATION_ENABLED),
                        config.get(SCHEMA_EVOLUTION_ENABLED),
                        changelogMode,
                        config.get(SINK_FLUSH_MAX_ROWS),
                        config.get(SINK_BATCH_MAX_ROWS_PER_COMMIT),
                        config.get(SINK_FLUSH_INTERVAL),
                        config.get(SINK_MAX_RETRIES),
                        config.get(SINK_RETRY_BACKOFF),
                        config.get(WRITE_MAX_ROWS_PER_FILE),
                        config.get(WRITE_MAX_ROWS_PER_GROUP),
                        config.get(WRITE_MAX_BYTES_PER_FILE),
                        config.get(WRITE_ENABLE_STABLE_ROW_IDS),
                        config.get(WRITE_MODE).trim().toUpperCase(Locale.ROOT),
                        zoneId,
                        LanceDbDataSinkOptions.getPropertiesByPrefix(
                                config, STORAGE_OPTIONS_PREFIX));
        return new LanceDbDataSink(sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROOT_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLE_PATH_MAPPING);
        options.add(TABLE_NAME_NORMALIZE_ENABLED);
        options.add(CREATE_TABLE_ENABLED);
        options.add(SCHEMA_VALIDATION_ENABLED);
        options.add(SCHEMA_EVOLUTION_ENABLED);
        options.add(SINK_CHANGELOG_MODE);
        options.add(SINK_FLUSH_MAX_ROWS);
        options.add(SINK_BATCH_MAX_ROWS_PER_COMMIT);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_RETRY_BACKOFF);
        options.add(WRITE_MAX_ROWS_PER_FILE);
        options.add(WRITE_MAX_ROWS_PER_GROUP);
        options.add(WRITE_MAX_BYTES_PER_FILE);
        options.add(WRITE_ENABLE_STABLE_ROW_IDS);
        options.add(WRITE_MODE);
        options.add(TIMEZONE);
        return options;
    }

    private Map<TableId, String> parseTablePathMapping(String value) {
        Map<TableId, String> mapping = new HashMap<>();
        if (value == null || value.trim().isEmpty()) {
            return mapping;
        }
        for (String pair : value.split(";")) {
            if (pair.trim().isEmpty()) {
                continue;
            }
            int separator = pair.indexOf(':');
            if (separator <= 0 || separator == pair.length() - 1) {
                throw new ValidationException(
                        TABLE_PATH_MAPPING.key()
                                + " entries must use sourceTable:targetPath format. Invalid entry: "
                                + pair);
            }
            TableId tableId = TableId.parse(pair.substring(0, separator).trim());
            String path = pair.substring(separator + 1).trim();
            validateNonBlank(path, TABLE_PATH_MAPPING.key());
            mapping.put(tableId, path);
        }
        validateDuplicateTargetPaths(mapping);
        return mapping;
    }

    private String normalizeChangelogMode(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        if (!"append-only".equals(normalized)
                && !"append-with-metadata".equals(normalized)
                && !"reject".equals(normalized)) {
            throw new ValidationException(
                    SINK_CHANGELOG_MODE.key()
                            + " must be one of append-only, append-with-metadata, reject.");
        }
        return normalized;
    }

    private void validateDuplicateTargetPaths(Map<TableId, String> mapping) {
        Map<String, TableId> owners = new HashMap<>();
        mapping.forEach(
                (tableId, path) -> {
                    String normalizedPath = LanceDbPathUtils.trimTrailingSlash(path);
                    TableId existing = owners.putIfAbsent(normalizedPath, tableId);
                    if (existing != null && !existing.equals(tableId)) {
                        throw new ValidationException(
                                TABLE_PATH_MAPPING.key()
                                        + " maps both "
                                        + existing
                                        + " and "
                                        + tableId
                                        + " to the same Lance dataset path "
                                        + normalizedPath
                                        + ".");
                    }
                });
    }

    private void validateWriteMode(String value) {
        String normalized = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
        try {
            WriteParams.WriteMode.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new ValidationException(
                    WRITE_MODE.key()
                            + " must be a valid Lance WriteParams.WriteMode value. Invalid value: "
                            + value,
                    e);
        }
    }

    private ZoneId parseZoneId(String value) {
        try {
            return ZoneId.of(value);
        } catch (ZoneRulesException e) {
            throw new ValidationException(TIMEZONE.key() + " is not a valid zone id: " + value, e);
        }
    }

    private void validateNonBlank(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException(key + " must not be blank.");
        }
    }

    private void validatePositive(int value, String key) {
        if (value <= 0) {
            throw new ValidationException(key + " must be greater than 0.");
        }
    }

    private void validatePositive(long value, String key) {
        if (value <= 0) {
            throw new ValidationException(key + " must be greater than 0.");
        }
    }

    private void validateNonNegative(int value, String key) {
        if (value < 0) {
            throw new ValidationException(key + " must be greater than or equal to 0.");
        }
    }

    private void validateNonNegative(long value, String key) {
        if (value < 0) {
            throw new ValidationException(key + " must be greater than or equal to 0.");
        }
    }
}
