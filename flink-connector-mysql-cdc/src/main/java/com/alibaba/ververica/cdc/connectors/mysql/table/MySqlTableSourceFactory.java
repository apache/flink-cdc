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

package com.alibaba.ververica.cdc.connectors.mysql.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.cdc.debezium.table.DebeziumOptions;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.DATABASE_NAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.HOSTNAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.PASSWORD;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.PORT;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_FETCH_SIZE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_OPTIMIZE_INTEGRAL_KEY;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SPLIT_COLUMN;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_SPLIT_SIZE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_MODE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_ID;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SERVER_TIME_ZONE;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SNAPSHOT_PARALLEL_SCAN;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.TABLE_NAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.USERNAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.validateAndGetServerId;
import static com.alibaba.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;

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
        int splitSize = config.get(SCAN_SPLIT_SIZE);
        int fetchSize = config.get(SCAN_FETCH_SIZE);
        ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));

        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        String serverId = validateAndGetServerId(config);
        boolean enableIntegralOptimization = config.get(SCAN_OPTIMIZE_INTEGRAL_KEY);
        boolean enableParallelRead = config.get(SNAPSHOT_PARALLEL_SCAN);
        String splitColumn = null;
        StartupOptions startupOptions = getStartupOptions(config);
        if (enableParallelRead) {
            validatePrimaryKeyIfEnableParallel(physicalSchema);
            splitColumn = validateAndGetSplitColumn(config.get(SCAN_SPLIT_COLUMN), physicalSchema);
            validateStartupOptionIfEnableParallel(startupOptions);
        }

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
                enableIntegralOptimization,
                enableParallelRead,
                splitSize,
                fetchSize,
                splitColumn,
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
        options.add(SNAPSHOT_PARALLEL_SCAN);
        options.add(SCAN_SPLIT_SIZE);
        options.add(SCAN_FETCH_SIZE);
        options.add(SCAN_SPLIT_COLUMN);
        options.add(SCAN_OPTIMIZE_INTEGRAL_KEY);
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

            case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                return StartupOptions.earliest();

            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
                String offsetFile = config.get(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
                int offsetPos = config.get(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
                return StartupOptions.specificOffset(offsetFile, offsetPos);

            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                long millis = config.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
                return StartupOptions.timestamp(millis);

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_EARLIEST,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private void validatePrimaryKeyIfEnableParallel(TableSchema physicalSchema) {
        if (!physicalSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    "The primary key is necessary when enable 'snapshot.parallel-scan' to 'true'");
        }
    }

    private String validateAndGetSplitColumn(String splitColumn, TableSchema physicalSchema) {
        String validatedSplitColumn = splitColumn;
        if (physicalSchema.getPrimaryKey().isPresent()) {
            int pkSize = physicalSchema.getPrimaryKey().get().getColumns().size();
            if (pkSize > 1) {
                Preconditions.checkState(
                        splitColumn != null,
                        "The 'scan.split.column' option is required if the primary key contains multiple fields");
                Preconditions.checkState(
                        physicalSchema.getPrimaryKey().get().getColumns().contains(splitColumn),
                        String.format(
                                "The 'scan.split.column' value %s should be one field of the primary key %s, but it does not.",
                                splitColumn, physicalSchema.getPrimaryKey().get().getColumns()));
                return splitColumn;
            }
            // single primary key field
            else {
                // use primary key by default
                if (splitColumn == null) {
                    validatedSplitColumn = physicalSchema.getPrimaryKey().get().getColumns().get(0);
                } else {
                    // validate configured split column
                    Preconditions.checkState(
                            physicalSchema.getPrimaryKey().get().getColumns().contains(splitColumn),
                            String.format(
                                    "The 'scan.split.column' value %s should be one field of the primary key %s, but it does not.",
                                    splitColumn,
                                    physicalSchema.getPrimaryKey().get().getColumns()));
                }
            }
        } else {
            throw new ValidationException(
                    "The primary key is necessary when enable 'snapshot.parallel-scan' to 'true'");
        }
        return validatedSplitColumn;
    }

    private void validateStartupOptionIfEnableParallel(StartupOptions startupOptions) {
        // validate mode
        Preconditions.checkState(
                startupOptions.startupMode == StartupMode.INITIAL,
                String.format(
                        "MySQL Parallel Source only supports startup mode 'initial' now,"
                                + " but actual is %s",
                        startupOptions.startupMode));
    }
}
