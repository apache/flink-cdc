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

package org.apache.flink.cdc.connectors.tdengine.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSink;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineNameUtils;
import org.apache.flink.table.api.ValidationException;

import java.time.Duration;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.CONNECTION_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.CREATE_DATABASE_ENABLED;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.CREATE_STABLE_ENABLED;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.DATABASE_NAME;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.DECIMAL_MAPPING;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.NAME_NORMALIZE_ENABLED;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_DELETE_MODE;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_FLUSH_MAX_ROWS;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_MAX_SQL_BYTES;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_RETRY_BACKOFF;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_SUBTABLE_CHANGE_MODE;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_TIMESTAMP_CHANGE_MODE;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SINK_UPDATE_MODE;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.STABLE_MAPPING;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.STABLE_NAME;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.STABLE_SCHEMA_VALIDATION_ENABLED;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.STRING_TYPE;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.SUBTABLE_FIELD;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.TAG_FIELDS;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.TIMESTAMP_FIELD;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.TIMEZONE;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.URL;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkOptions.VARCHAR_MAX_LENGTH_DEFAULT;

/** Factory for creating configured {@link TDengineDataSink} instances. */
@Internal
public class TDengineDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "tdengine";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(CONNECTION_PROPERTIES_PREFIX);

        Configuration config = context.getFactoryConfiguration();
        validateJdbcUrl(config.get(URL));
        validateNonBlank(config.get(DATABASE_NAME), DATABASE_NAME.key());
        validateNonBlank(config.get(USERNAME), USERNAME.key());
        validateNonBlank(config.get(TIMESTAMP_FIELD), TIMESTAMP_FIELD.key());
        validateNonBlank(config.get(SUBTABLE_FIELD), SUBTABLE_FIELD.key());
        validatePositive(config.get(SINK_FLUSH_MAX_ROWS), SINK_FLUSH_MAX_ROWS.key());
        validatePositive(config.get(SINK_MAX_SQL_BYTES), SINK_MAX_SQL_BYTES.key());
        validatePositive(config.get(VARCHAR_MAX_LENGTH_DEFAULT), VARCHAR_MAX_LENGTH_DEFAULT.key());
        validatePositiveDuration(config.get(SINK_FLUSH_INTERVAL), SINK_FLUSH_INTERVAL.key());
        validateNonNegative(config.get(SINK_MAX_RETRIES), SINK_MAX_RETRIES.key());
        validateNonNegativeDuration(config.get(SINK_RETRY_BACKOFF), SINK_RETRY_BACKOFF.key());

        boolean normalizeNames = config.get(NAME_NORMALIZE_ENABLED);
        String databaseName =
                normalizeOrValidate(config.get(DATABASE_NAME), "database", normalizeNames);
        String stableName =
                config.get(STABLE_NAME) == null || config.get(STABLE_NAME).trim().isEmpty()
                        ? ""
                        : normalizeOrValidate(config.get(STABLE_NAME), "stable", normalizeNames);

        List<String> tagFields = parseTagFields(config.get(TAG_FIELDS));
        if (tagFields.isEmpty()) {
            throw new ValidationException(
                    TAG_FIELDS.key() + " must contain at least one tag field.");
        }
        validateFieldRoles(config.get(TIMESTAMP_FIELD), config.get(SUBTABLE_FIELD), tagFields);

        TDengineDataSinkConfig sinkConfig =
                new TDengineDataSinkConfig(
                        config.get(URL).trim(),
                        config.get(USERNAME),
                        config.get(PASSWORD),
                        databaseName,
                        stableName,
                        parseStableMappings(config.get(STABLE_MAPPING), normalizeNames),
                        normalizeNames,
                        config.get(TIMESTAMP_FIELD).trim(),
                        config.get(SUBTABLE_FIELD).trim(),
                        tagFields,
                        config.get(CREATE_DATABASE_ENABLED),
                        config.get(CREATE_STABLE_ENABLED),
                        config.get(STABLE_SCHEMA_VALIDATION_ENABLED),
                        config.get(VARCHAR_MAX_LENGTH_DEFAULT),
                        normalizeEnumValue(
                                config.get(STRING_TYPE), STRING_TYPE.key(), "nchar", "varchar"),
                        normalizeEnumValue(
                                config.get(DECIMAL_MAPPING),
                                DECIMAL_MAPPING.key(),
                                "double",
                                "nchar"),
                        parseZoneId(config.get(TIMEZONE)),
                        config.get(SINK_FLUSH_MAX_ROWS),
                        config.get(SINK_MAX_SQL_BYTES),
                        config.get(SINK_FLUSH_INTERVAL),
                        config.get(SINK_MAX_RETRIES),
                        config.get(SINK_RETRY_BACKOFF),
                        normalizeEnumValue(
                                config.get(SINK_DELETE_MODE),
                                SINK_DELETE_MODE.key(),
                                "reject",
                                "ignore"),
                        normalizeEnumValue(
                                config.get(SINK_UPDATE_MODE),
                                SINK_UPDATE_MODE.key(),
                                "upsert",
                                "reject"),
                        normalizeEnumValue(
                                config.get(SINK_TIMESTAMP_CHANGE_MODE),
                                SINK_TIMESTAMP_CHANGE_MODE.key(),
                                "reject",
                                "allow"),
                        normalizeEnumValue(
                                config.get(SINK_SUBTABLE_CHANGE_MODE),
                                SINK_SUBTABLE_CHANGE_MODE.key(),
                                "reject",
                                "allow"),
                        parseConnectionProperties(config));
        return new TDengineDataSink(sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(DATABASE_NAME);
        options.add(TIMESTAMP_FIELD);
        options.add(SUBTABLE_FIELD);
        options.add(TAG_FIELDS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(STABLE_NAME);
        options.add(STABLE_MAPPING);
        options.add(NAME_NORMALIZE_ENABLED);
        options.add(CREATE_DATABASE_ENABLED);
        options.add(CREATE_STABLE_ENABLED);
        options.add(STABLE_SCHEMA_VALIDATION_ENABLED);
        options.add(VARCHAR_MAX_LENGTH_DEFAULT);
        options.add(STRING_TYPE);
        options.add(DECIMAL_MAPPING);
        options.add(TIMEZONE);
        options.add(SINK_FLUSH_MAX_ROWS);
        options.add(SINK_MAX_SQL_BYTES);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_RETRY_BACKOFF);
        options.add(SINK_DELETE_MODE);
        options.add(SINK_UPDATE_MODE);
        options.add(SINK_TIMESTAMP_CHANGE_MODE);
        options.add(SINK_SUBTABLE_CHANGE_MODE);
        return options;
    }

    private void validateJdbcUrl(String url) {
        if (url == null || !url.trim().startsWith("jdbc:TAOS-WS://")) {
            throw new ValidationException(URL.key() + " must start with jdbc:TAOS-WS://.");
        }
    }

    private String normalizeOrValidate(String value, String role, boolean normalize) {
        return normalize
                ? TDengineNameUtils.normalizeIdentifier(value, role)
                : TDengineNameUtils.validateIdentifier(value, role);
    }

    private Map<TableId, String> parseStableMappings(String value, boolean normalize) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TableId, String> mappings = new LinkedHashMap<>();
        for (String item : value.split(";")) {
            if (item.trim().isEmpty()) {
                continue;
            }
            String[] parts = item.split(":", 2);
            if (parts.length != 2 || parts[0].trim().isEmpty() || parts[1].trim().isEmpty()) {
                throw new ValidationException(
                        STABLE_MAPPING.key()
                                + " is malformed. Expected tableId:stable;tableId2:stable2.");
            }
            TableId tableId = TableId.parse(parts[0].trim());
            if (mappings.containsKey(tableId)) {
                throw new ValidationException(
                        STABLE_MAPPING.key()
                                + " contains duplicate mapping for table "
                                + tableId
                                + ".");
            }
            mappings.put(tableId, normalizeOrValidate(parts[1].trim(), "stable", normalize));
        }
        return mappings;
    }

    private List<String> parseTagFields(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> fields = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String item : value.split(",")) {
            String field = item.trim();
            if (field.isEmpty()) {
                continue;
            }
            if (!seen.add(field)) {
                throw new ValidationException(
                        TAG_FIELDS.key() + " contains duplicate field " + field + ".");
            }
            fields.add(field);
        }
        return Collections.unmodifiableList(fields);
    }

    private void validateFieldRoles(
            String timestampField, String subtableField, List<String> tagFields) {
        String trimmedTimestampField = timestampField.trim();
        String trimmedSubtableField = subtableField.trim();
        if (trimmedTimestampField.equals(trimmedSubtableField)) {
            throw new ValidationException(
                    TIMESTAMP_FIELD.key()
                            + " and "
                            + SUBTABLE_FIELD.key()
                            + " must refer to different columns.");
        }
        if (tagFields.contains(trimmedTimestampField)) {
            throw new ValidationException(
                    TIMESTAMP_FIELD.key() + " must not be listed in " + TAG_FIELDS.key() + ".");
        }
        if (tagFields.contains(trimmedSubtableField)) {
            throw new ValidationException(
                    SUBTABLE_FIELD.key() + " must not be listed in " + TAG_FIELDS.key() + ".");
        }
    }

    private ZoneId parseZoneId(String value) {
        try {
            return ZoneId.of(value);
        } catch (ZoneRulesException e) {
            throw new ValidationException("Invalid timezone: " + value, e);
        }
    }

    private Map<String, String> parseConnectionProperties(Configuration config) {
        Map<String, String> properties = new HashMap<>();
        TDengineDataSinkOptions.getPropertiesByPrefix(config, CONNECTION_PROPERTIES_PREFIX)
                .forEach(
                        (key, value) -> {
                            validateNonBlank(key, CONNECTION_PROPERTIES_PREFIX + "<key>");
                            properties.put(key.trim(), value);
                        });
        return properties;
    }

    private String normalizeEnumValue(String value, String key, String... allowedValues) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        for (String allowedValue : allowedValues) {
            if (allowedValue.equals(normalized)) {
                return normalized;
            }
        }
        throw new ValidationException(
                key
                        + " must be one of "
                        + String.join(", ", allowedValues)
                        + ", but was "
                        + value
                        + ".");
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

    private void validateNonNegative(int value, String key) {
        if (value < 0) {
            throw new ValidationException(key + " must be greater than or equal to 0.");
        }
    }

    private void validatePositiveDuration(Duration value, String key) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new ValidationException(key + " must be greater than 0.");
        }
    }

    private void validateNonNegativeDuration(Duration value, String key) {
        if (value == null || value.isNegative()) {
            throw new ValidationException(key + " must be greater than or equal to 0.");
        }
    }
}
