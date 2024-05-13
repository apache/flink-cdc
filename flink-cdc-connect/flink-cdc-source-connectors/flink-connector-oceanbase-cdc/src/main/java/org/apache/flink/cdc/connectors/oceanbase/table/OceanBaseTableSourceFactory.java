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

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils;
import org.apache.flink.cdc.connectors.oceanbase.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;

/** Factory for creating configured instance of {@link OceanBaseTableSource}. */
public class OceanBaseTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "oceanbase-cdc";

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to be used when connecting to OceanBase.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to be used when connecting to OceanBase.");

    public static final ConfigOption<String> TENANT_NAME =
            ConfigOptions.key("tenant-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tenant name of OceanBase to monitor.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Database name of OceanBase to monitor, should be regular expression. Only can be used with 'initial' mode.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table name of OceanBase to monitor, should be regular expression. Only can be used with 'initial' mode.");

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of full names of tables, separated by commas, e.g. \"db1.table1, db2.table2\".");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("+00:00")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the database server or log proxy server before timing out.");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "IP address or hostname of the OceanBase database server or OceanBase proxy server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Integer port number of OceanBase database server or OceanBase proxy server.");

    public static final ConfigOption<String> COMPATIBLE_MODE =
            ConfigOptions.key("compatible-mode")
                    .stringType()
                    .defaultValue("mysql")
                    .withDescription(
                            "The compatible mode of OceanBase, can be 'mysql' or 'oracle'.");

    public static final ConfigOption<String> JDBC_DRIVER =
            ConfigOptions.key("jdbc.driver")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.mysql.cj.jdbc.Driver' by default.");

    public static final ConfigOption<String> LOG_PROXY_HOST =
            ConfigOptions.key("logproxy.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of OceanBase log proxy service.");

    public static final ConfigOption<Integer> LOG_PROXY_PORT =
            ConfigOptions.key("logproxy.port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Port number of OceanBase log proxy service.");

    public static final ConfigOption<String> LOG_PROXY_CLIENT_ID =
            ConfigOptions.key("logproxy.client.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Id of log proxy client, used to distinguish different connections.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP =
            ConfigOptions.key("scan.startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp in seconds used in case of \"timestamp\" startup mode.");

    public static final ConfigOption<String> RS_LIST =
            ConfigOptions.key("rootserver-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The semicolon-separated list of root servers in format `ip:rpc_port:sql_port`, corresponding to the parameter 'rootservice_list' in the database.");

    public static final ConfigOption<String> CONFIG_URL =
            ConfigOptions.key("config-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The url used to get root servers list, corresponding to the parameter 'obconfig_url' in the database.");

    public static final ConfigOption<String> WORKING_MODE =
            ConfigOptions.key("working-mode")
                    .stringType()
                    .defaultValue("storage")
                    .withDescription(
                            "The working mode of 'obcdc', can be `storage` (default value, supported from `obcdc` 3.1.3) or `memory`.");

    public static final String OBCDC_PROPERTIES_PREFIX = "obcdc.properties.";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(
                JdbcUrlUtils.PROPERTIES_PREFIX,
                OBCDC_PROPERTIES_PREFIX,
                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        ReadableConfig config = helper.getOptions();

        StartupOptions startupOptions = getStartupOptions(config);

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String tenantName = config.get(TENANT_NAME);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String tableList = config.get(TABLE_LIST);

        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);

        String hostname = config.get(HOSTNAME);
        Integer port = config.get(PORT);
        String compatibleMode = config.get(COMPATIBLE_MODE);
        String jdbcDriver = config.get(JDBC_DRIVER);

        validateJdbcDriver(compatibleMode, jdbcDriver);

        String logProxyHost = config.get(LOG_PROXY_HOST);
        Integer logProxyPort = config.get(LOG_PROXY_PORT);
        String logProxyClientId = config.get(LOG_PROXY_CLIENT_ID);
        Long startupTimestamp = config.get(SCAN_STARTUP_TIMESTAMP);
        String rsList = config.get(RS_LIST);
        String configUrl = config.get(CONFIG_URL);
        String workingMode = config.get(WORKING_MODE);

        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return new OceanBaseTableSource(
                physicalSchema,
                startupOptions,
                username,
                password,
                tenantName,
                databaseName,
                tableName,
                tableList,
                serverTimeZone,
                connectTimeout,
                hostname,
                port,
                compatibleMode,
                jdbcDriver,
                JdbcUrlUtils.getJdbcProperties(context.getCatalogTable().getOptions()),
                logProxyHost,
                logProxyPort,
                logProxyClientId,
                startupTimestamp,
                rsList,
                configUrl,
                workingMode,
                getProperties(context.getCatalogTable().getOptions(), OBCDC_PROPERTIES_PREFIX),
                DebeziumOptions.getDebeziumProperties(context.getCatalogTable().getOptions()));
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
        options.add(HOSTNAME);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP);
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(TABLE_LIST);
        options.add(COMPATIBLE_MODE);
        options.add(JDBC_DRIVER);
        options.add(CONNECT_TIMEOUT);
        options.add(SERVER_TIME_ZONE);
        options.add(TENANT_NAME);
        options.add(LOG_PROXY_HOST);
        options.add(LOG_PROXY_PORT);
        options.add(LOG_PROXY_CLIENT_ID);
        options.add(RS_LIST);
        options.add(CONFIG_URL);
        options.add(WORKING_MODE);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                if (config.get(SCAN_STARTUP_TIMESTAMP) != null) {
                    return StartupOptions.timestamp(config.get(SCAN_STARTUP_TIMESTAMP) * 1000);
                }
                throw new ValidationException(
                        String.format(
                                "Option '%s' should not be empty", SCAN_STARTUP_TIMESTAMP.key()));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private void validateJdbcDriver(String compatibleMode, String jdbcDriver) {
        Objects.requireNonNull(compatibleMode, "'compatible-mode' is required.");
        Objects.requireNonNull(jdbcDriver, "'jdbc.driver' is required.");
        if ("oracle".equalsIgnoreCase(compatibleMode)
                && !OceanBaseUtils.isOceanBaseDriver(jdbcDriver)) {
            throw new IllegalArgumentException(
                    "OceanBase JDBC driver is required for OceanBase Oracle mode.");
        }
        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Jdbc driver class not found", e);
        }
    }

    private Properties getProperties(Map<String, String> tableOptions, String prefix) {
        Properties properties = new Properties();
        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring(prefix.length());
                            properties.put(subKey, value);
                        });
        return properties;
    }
}
