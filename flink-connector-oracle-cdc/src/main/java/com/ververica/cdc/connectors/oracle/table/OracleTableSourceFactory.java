/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.ververica.cdc.debezium.table.DebeziumOptions;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.base.options.JdbcSourceOptions.USERNAME;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions.PORT;
import static com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions.SCHEMA_NAME;
import static com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions.URL;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for creating configured instance of {@link OracleTableSource}. */
public class OracleTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "oracle-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String url = config.get(URL);
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        int port = config.get(PORT);
        StartupOptions startupOptions = getStartupOptions(config);
        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);

        if (enableParallelRead) {
            validateIntegerOption(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
            validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
            validateIntegerOption(CONNECTION_POOL_SIZE, connectionPoolSize, 1);
            validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        }

        return new OracleTableSource(
                physicalSchema,
                url,
                port,
                hostname,
                databaseName,
                tableName,
                schemaName,
                username,
                password,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                startupOptions,
                enableParallelRead);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(SCHEMA_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(URL);
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(CONNECT_MAX_RETRIES);
        options.add(CONNECTION_POOL_SIZE);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                modeString));
        }
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }
}
