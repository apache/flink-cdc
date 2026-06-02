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

package org.apache.flink.cdc.connectors.pgvector.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSink;
import org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkConfig;
import org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorColumnSpec;
import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.CREATE_EXTENSION_ENABLED;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.CREATE_SCHEMA_ENABLED;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.CREATE_TABLE_ENABLED;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.DEFAULT_SCHEMA;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.JDBC_URL;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.SINK_ALLOW_NO_PRIMARY_KEY;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.SINK_DELETE_ENABLED;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.SINK_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.SINK_FLUSH_MAX_ROWS;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.SINK_RETRY_BACKOFF;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSinkOptions.VECTOR_COLUMNS_PREFIX;

/** Factory for creating configured {@link PgVectorDataSink} instances. */
@Internal
public class PgVectorDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "pgvector";
    private static final Pattern STORAGE_PARAMETER_PATTERN =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(VECTOR_COLUMNS_PREFIX, TABLE_CREATE_PROPERTIES_PREFIX);

        Configuration config = context.getFactoryConfiguration();
        validatePositive(config.get(SINK_FLUSH_MAX_ROWS), SINK_FLUSH_MAX_ROWS.key());
        validatePositive(config.get(SINK_FLUSH_INTERVAL).toMillis(), SINK_FLUSH_INTERVAL.key());
        validateNonNegative(config.get(SINK_MAX_RETRIES), SINK_MAX_RETRIES.key());
        validateNonNegative(config.get(SINK_RETRY_BACKOFF).toMillis(), SINK_RETRY_BACKOFF.key());
        validateJdbcUrl(config.get(JDBC_URL));
        validateNonBlank(config.get(DEFAULT_SCHEMA), DEFAULT_SCHEMA.key());
        Map<String, String> tableCreateProperties =
                PgVectorDataSinkOptions.getPropertiesByPrefix(
                        config, TABLE_CREATE_PROPERTIES_PREFIX);
        validateTableCreateProperties(tableCreateProperties);

        PgVectorDataSinkConfig sinkConfig =
                new PgVectorDataSinkConfig(
                        config.get(JDBC_URL),
                        config.get(USERNAME),
                        config.get(PASSWORD),
                        config.get(DEFAULT_SCHEMA).trim(),
                        config.get(CREATE_SCHEMA_ENABLED),
                        config.get(CREATE_TABLE_ENABLED),
                        config.get(CREATE_EXTENSION_ENABLED),
                        config.get(SINK_FLUSH_MAX_ROWS),
                        config.get(SINK_FLUSH_INTERVAL),
                        config.get(SINK_MAX_RETRIES),
                        config.get(SINK_RETRY_BACKOFF),
                        config.get(SINK_DELETE_ENABLED),
                        config.get(SINK_ALLOW_NO_PRIMARY_KEY),
                        parseVectorColumns(config),
                        tableCreateProperties);
        return new PgVectorDataSink(sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(JDBC_URL);
        options.add(USERNAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PASSWORD);
        options.add(DEFAULT_SCHEMA);
        options.add(CREATE_SCHEMA_ENABLED);
        options.add(CREATE_TABLE_ENABLED);
        options.add(CREATE_EXTENSION_ENABLED);
        options.add(SINK_FLUSH_MAX_ROWS);
        options.add(SINK_FLUSH_INTERVAL);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_RETRY_BACKOFF);
        options.add(SINK_DELETE_ENABLED);
        options.add(SINK_ALLOW_NO_PRIMARY_KEY);
        return options;
    }

    private Map<String, PgVectorColumnSpec> parseVectorColumns(Configuration config) {
        Map<String, PgVectorColumnSpec> vectorColumns = new HashMap<>();
        PgVectorDataSinkOptions.getPropertiesByPrefix(config, VECTOR_COLUMNS_PREFIX)
                .forEach(
                        (columnKey, typeDefinition) -> {
                            if (columnKey.trim().isEmpty()) {
                                throw new ValidationException(
                                        "Vector column key must not be empty.");
                            }
                            try {
                                vectorColumns.put(
                                        columnKey.trim(),
                                        PgVectorColumnSpec.parse(typeDefinition.trim()));
                            } catch (IllegalArgumentException e) {
                                throw new ValidationException(
                                        "Invalid vector column definition for '"
                                                + VECTOR_COLUMNS_PREFIX
                                                + columnKey
                                                + "'.",
                                        e);
                            }
                        });
        return vectorColumns;
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

    private void validateJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.trim().startsWith("jdbc:postgresql:")) {
            throw new ValidationException(
                    JDBC_URL.key()
                            + " must be a PostgreSQL JDBC URL starting with jdbc:postgresql:");
        }
    }

    private void validateNonBlank(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException(key + " must not be blank.");
        }
    }

    private void validateTableCreateProperties(Map<String, String> properties) {
        properties.forEach(
                (key, value) -> {
                    if (!STORAGE_PARAMETER_PATTERN.matcher(key).matches()) {
                        throw new ValidationException(
                                "Invalid PostgreSQL table create property key: " + key);
                    }
                    validateNonBlank(value, TABLE_CREATE_PROPERTIES_PREFIX + key);
                });
    }
}
