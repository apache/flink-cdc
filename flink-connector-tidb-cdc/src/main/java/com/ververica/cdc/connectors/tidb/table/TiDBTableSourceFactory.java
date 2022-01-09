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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;

import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.DATABASE_NAME;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.HOSTNAME;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.PASSWORD;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.SCAN_STARTUP_MODE;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.TABLE_NAME;
import static com.ververica.cdc.connectors.tidb.TDBSourceOptions.USERNAME;

/** Factory for creating configured instance of {@link TiDBTableSource}. */
public class TiDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "tidb-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        StartupOptions startupOptions = getStartupOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new TiDBTableSource(
                physicalSchema,
                hostname,
                databaseName,
                tableName,
                username,
                password,
                startupOptions,
                context.getCatalogTable().getOptions());
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
        options.add(TDBSourceOptions.TIKV_PD_ADDRESSES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(TDBSourceOptions.TIKV_GRPC_TIMEOUT);
        options.add(TDBSourceOptions.TIKV_GRPC_SCAN_TIMEOUT);
        options.add(TDBSourceOptions.TIKV_BATCH_GET_CONCURRENCY);
        options.add(TDBSourceOptions.TIKV_BATCH_PUT_CONCURRENCY);
        options.add(TDBSourceOptions.TIKV_BATCH_SCAN_CONCURRENCY);
        options.add(TDBSourceOptions.TIKV_BATCH_DELETE_CONCURRENCY);
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
}
