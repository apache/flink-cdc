/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.debezium.table.DebeziumOptions;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.PORT;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_ID;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_TIME_ZONE;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.USERNAME;
import static com.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.validateAndGetServerId;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.util.Preconditions.checkState;

/** Factory for creating configured instance of {@link MySqlTableSource}. */
public class MySqlTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "mysql-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        int port = config.get(PORT);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        String serverId = validateAndGetServerId(config);
        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        StartupOptions startupOptions = getStartupOptions(config);
        if (enableParallelRead) {
            validatePrimaryKeyIfEnableParallel(physicalSchema);
            validateStartupOptionIfEnableParallel(startupOptions);
        }
        validateSplitSize(splitSize);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);

        return new MySqlTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                tableName,
                username,
                password,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                serverId,
                enableParallelRead,
                splitSize,
                fetchSize,
                connectTimeout,
                startupOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PORT);
        options.add(SERVER_TIME_ZONE);
        options.add(SERVER_ID);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                throw new ValidationException(
                        String.format(
                                "Unsupported option value '%s', the options [%s, %s, %s] are not supported correctly, please do not use them until they're correctly supported",
                                modeString,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP));

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

    private void validatePrimaryKeyIfEnableParallel(TableSchema physicalSchema) {
        if (!physicalSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    String.format(
                            "The primary key is necessary when enable '%s' to 'true'",
                            SCAN_INCREMENTAL_SNAPSHOT_ENABLED));
        }
    }

    private void validateStartupOptionIfEnableParallel(StartupOptions startupOptions) {
        // validate mode
        Preconditions.checkState(
                startupOptions.startupMode == StartupMode.INITIAL
                        || startupOptions.startupMode == StartupMode.LATEST_OFFSET,
                String.format(
                        "MySql Parallel Source only supports startup mode 'initial' and 'latest-offset',"
                                + " but actual is %s",
                        startupOptions.startupMode));
    }

    private void validateSplitSize(int splitSize) {
        checkState(
                splitSize > 1,
                String.format(
                        "The value of option '%s' must larger than 1, but is %d",
                        SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key(), splitSize));
    }
}
