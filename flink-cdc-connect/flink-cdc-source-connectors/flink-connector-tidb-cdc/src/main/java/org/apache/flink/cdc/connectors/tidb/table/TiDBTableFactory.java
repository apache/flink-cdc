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

package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.DATABASE_NAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.TABLE_NAME;
import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND;
import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.HOST_MAPPING;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.JDBC_DRIVER;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.PD_ADDRESSES;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.TABLE_LIST;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.TIDB_PORT;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;

/** Factory for creating configured instances of {@link TiDBTableSource}. */
public class TiDBTableFactory implements DynamicTableSourceFactory {
    private static final String IDENTIFIER = "tidb-cdc";

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
        options.add(PD_ADDRESSES);
        options.add(TIDB_PORT);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);

        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(TABLE_LIST);
        options.add(CONNECT_TIMEOUT);
        options.add(SERVER_TIME_ZONE);
        options.add(HOST_MAPPING);
        options.add(JDBC_DRIVER);
        options.add(HEARTBEAT_INTERVAL);

        //      increment snapshot options
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CONNECTION_POOL_SIZE);
        options.add(CONNECT_MAX_RETRIES);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);
        Long startupTimestamp = config.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(startupTimestamp);
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        // 作用
        helper.validateExcept(
                JdbcUrlUtils.PROPERTIES_PREFIX,
                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX,
                TiKVOptions.TIKV_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();

        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String tableList = config.get(TABLE_LIST);

        int port = config.get(TIDB_PORT);
        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        String pdAddresses = config.get(PD_ADDRESSES);
        String hostMapping = config.get(HOST_MAPPING);
        String jdbcDriver = config.get(JDBC_DRIVER);

        //  increment snapshot options
        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        String chunkKeyColumn =
                config.getOptional(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN).orElse(null);
        Map<ObjectPath, String> chunkKeyColumns = new HashMap<>();
        if (chunkKeyColumn != null) {
            chunkKeyColumns.put(new ObjectPath(databaseName, tableName), chunkKeyColumn);
        }

        double distributionFactorUpper = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        StartupOptions startupOptions = getStartupOptions(config);

        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        return new TiDBTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                tableName,
                tableList,
                username,
                password,
                serverTimeZone,
                getDebeziumProperties(context.getCatalogTable().getOptions()),
                enableParallelRead,
                heartbeatInterval,
                pdAddresses,
                hostMapping,
                connectTimeout,
                TiKVOptions.getTiKVOptions(context.getCatalogTable().getOptions()),
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                chunkKeyColumn,
                chunkKeyColumns,
                jdbcDriver,
                startupOptions);
    }

    static class TiKVOptions {
        private static final String TIKV_OPTIONS_PREFIX = "tikv.";

        public static Map<String, String> getTiKVOptions(Map<String, String> properties) {
            Map<String, String> tikvOptions = new HashMap<>();

            if (hasTiKVOptions(properties)) {
                properties.keySet().stream()
                        .filter(key -> key.startsWith(TIKV_OPTIONS_PREFIX))
                        .forEach(
                                key -> {
                                    final String value = properties.get(key);
                                    tikvOptions.put(key, value);
                                });
            }
            return tikvOptions;
        }

        /**
         * Decides if the table options contains Debezium client properties that start with prefix
         * 'debezium'.
         */
        private static boolean hasTiKVOptions(Map<String, String> options) {
            return options.keySet().stream().anyMatch(k -> k.startsWith(TIKV_OPTIONS_PREFIX));
        }
    }
}
