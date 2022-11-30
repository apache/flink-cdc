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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.PD_ADDRESSES;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.TIKV_BATCH_GET_CONCURRENCY;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.TIKV_BATCH_SCAN_CONCURRENCY;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.TIKV_GRPC_SCAN_TIMEOUT;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.TIKV_GRPC_TIMEOUT;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;

/** Factory for creating configured instance of {@link TiDBTableSource}. */
public class TiDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "tidb-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig config = helper.getOptions();
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String pdAddresses = config.get(PD_ADDRESSES);
        StartupOptions startupOptions = getStartupOptions(config);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        return new TiDBTableSource(
                physicalSchema,
                databaseName,
                tableName,
                pdAddresses,
                startupOptions,
                TiKVOptions.getTiKVOptions(context.getCatalogTable().getOptions()));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(PD_ADDRESSES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(TIKV_GRPC_TIMEOUT);
        options.add(TIKV_GRPC_SCAN_TIMEOUT);
        options.add(TIKV_BATCH_GET_CONCURRENCY);
        options.add(TIKV_BATCH_SCAN_CONCURRENCY);
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
