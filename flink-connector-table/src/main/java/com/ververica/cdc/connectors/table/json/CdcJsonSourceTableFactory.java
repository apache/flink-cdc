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

package com.ververica.cdc.connectors.table.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ververica.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static com.ververica.cdc.debezium.table.DebeziumOptions.getDebeziumProperties;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;

/**
 * The table type reads cdc and returns cdc json.
 */
public class CdcJsonSourceTableFactory implements DynamicTableSourceFactory {


    private static final String IDENTIFIER = "cdc-json";

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the PostgreSQL database server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Integer port number of the PostgreSQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the PostgreSQL database to use when connecting to the PostgreSQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the PostgreSQL database server.");

    public static final ConfigOption<String> DATABASE_LIST =
            ConfigOptions.key("database-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the PostgreSQL server to monitor.");

    public static final ConfigOption<String> SCHEMA_LIST =
            ConfigOptions.key("schema-list")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Schema name of the PostgreSQL database to monitor.");

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the PostgreSQL database to monitor.");

    public static final ConfigOption<String> SOURCE_TYPE =
            ConfigOptions.key("source-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the PostgreSQL database to monitor.");

    public static final ConfigOption<String> EXTRA_PROP =
            ConfigOptions.key("extra-prop.")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the PostgreSQL database to monitor.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DEBEZIUM_OPTIONS_PREFIX, EXTRA_PROP.key());

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        List<String> databaseList = Arrays.asList(config.get(DATABASE_LIST).split(","));
        String sourceType = config.get(SOURCE_TYPE);
        List<String> schemaList = Arrays.asList(config.get(SCHEMA_LIST).split(","));
        List<String> tableList = Arrays.asList(config.get(TABLE_LIST).split(","));
        int port = config.get(PORT);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        return new CdcJsonSourceTable(
                physicalSchema,
                port,
                hostname,
                databaseList,
                schemaList,
                tableList,
                username,
                password,
                sourceType,
                getExtraProperties(context.getCatalogTable().getOptions()),
                getDebeziumProperties(getExtraProperties(context.getCatalogTable().getOptions())));
    }

    public static Map<String, String> getExtraProperties(Map<String, String> properties) {
        final Map<String, String> extraProperties = new HashMap<String, String>();

        properties.keySet().stream()
                .filter(key -> key.startsWith(EXTRA_PROP.key()))
                .forEach(
                        key -> {
                            final String value = properties.get(key);
                            final String subKey =
                                    key.substring((EXTRA_PROP.key()).length());
                            extraProperties.put(subKey, value);
                        });
        return extraProperties;
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
        options.add(DATABASE_LIST);
        options.add(TABLE_LIST);
        options.add(SOURCE_TYPE);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCHEMA_LIST);
        return options;
    }
}
