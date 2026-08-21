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

package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.common.source.discover.TableDiscovererFactory;
import org.apache.flink.cdc.connectors.fluss.source.FlussDataSource;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.CLIENT_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SCAN_DISCOVERY_INTERVAL;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SCAN_STARTUP_TIMESTAMP;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.TABLE_DISCOVERER_TYPE;

/** Factory for creating configured instances of {@link FlussDataSource}. */
public class FlussDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "fluss";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(CLIENT_PROPERTIES_PREFIX, TABLE_DISCOVERER_OPTIONS_PREFIX);

        org.apache.flink.cdc.common.configuration.Configuration factoryConfiguration =
                context.getFactoryConfiguration();

        String startupMode = factoryConfiguration.get(SCAN_STARTUP_MODE);

        Configuration flussConfig = toFlussClientConfig(factoryConfiguration);

        TableDiscoverer discoverer =
                createDiscoverer(factoryConfiguration, context.getClassLoader());

        OffsetsInitializer offsetsInitializer =
                getOffsetsInitializer(startupMode, factoryConfiguration);

        long scanDiscoveryIntervalMs = factoryConfiguration.get(SCAN_DISCOVERY_INTERVAL).toMillis();

        return new FlussDataSource(
                flussConfig,
                factoryConfiguration,
                discoverer,
                offsetsInitializer,
                scanDiscoveryIntervalMs);
    }

    private static TableDiscoverer createDiscoverer(
            org.apache.flink.cdc.common.configuration.Configuration config,
            ClassLoader classLoader) {
        String type = config.get(TABLE_DISCOVERER_TYPE);
        return TableDiscovererFactory.createDiscoverer(type, classLoader);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TABLE_DISCOVERER_TYPE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP);
        options.add(SCAN_DISCOVERY_INTERVAL);
        return options;
    }

    private static OffsetsInitializer getOffsetsInitializer(
            String startupMode, org.apache.flink.cdc.common.configuration.Configuration config) {
        if ("earliest".equalsIgnoreCase(startupMode)) {
            return OffsetsInitializer.earliest();
        } else if ("latest".equalsIgnoreCase(startupMode)) {
            return OffsetsInitializer.latest();
        } else if ("full".equalsIgnoreCase(startupMode)) {
            return OffsetsInitializer.full();
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            String timestampStr = config.get(SCAN_STARTUP_TIMESTAMP);
            if (timestampStr == null || timestampStr.isEmpty()) {
                throw new IllegalArgumentException(
                        "'scan.startup.timestamp' is required when scan.startup.mode is 'timestamp'.");
            }
            long timestampMs = parseTimestamp(timestampStr);
            return OffsetsInitializer.timestamp(timestampMs);
        } else {
            throw new IllegalArgumentException("Unsupported startup mode: " + startupMode);
        }
    }

    /**
     * Parses a timestamp string to a long value. Supports both epoch milliseconds and 'yyyy-MM-dd
     * HH:mm:ss' format.
     */
    private static long parseTimestamp(String timestampStr) {
        if (timestampStr.matches("\\d+")) {
            return Long.parseLong(timestampStr);
        }
        try {
            return java.time.LocalDateTime.parse(
                            timestampStr,
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    .atZone(java.time.ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid 'scan.startup.timestamp' value '%s'. "
                                    + "Expected format: 'yyyy-MM-dd HH:mm:ss' or epoch milliseconds.",
                            timestampStr),
                    e);
        }
    }

    private static Configuration toFlussClientConfig(
            org.apache.flink.cdc.common.configuration.Configuration factoryConfig) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                ConfigOptions.BOOTSTRAP_SERVERS.key(), factoryConfig.get(BOOTSTRAP_SERVERS));

        factoryConfig
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(CLIENT_PROPERTIES_PREFIX)) {
                                flussConfig.setString(key.substring("properties.".length()), value);
                            }
                        });
        return flussConfig;
    }
}
