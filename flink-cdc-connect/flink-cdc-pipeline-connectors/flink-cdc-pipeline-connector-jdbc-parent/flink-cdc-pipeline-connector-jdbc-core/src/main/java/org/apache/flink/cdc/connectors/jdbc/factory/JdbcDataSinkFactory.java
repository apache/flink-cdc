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

package org.apache.flink.cdc.connectors.jdbc.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialectFactory;
import org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions;
import org.apache.flink.cdc.connectors.jdbc.sink.JdbcDataSink;
import org.apache.flink.configuration.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.CONNECT_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.CONNECT_TIMEOUT;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.CONN_URL;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.DRIVER_CLASS_NAME;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.JDBC_PROPERTIES_PROP_PREFIX;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.WRITE_BATCH_INTERVAL_MS;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.WRITE_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions.WRITE_MAX_RETRIES;

/** A {@link DataSinkFactory} for creating JDBC sinks. */
public class JdbcDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "jdbc";

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSinkFactory.class);

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(JDBC_PROPERTIES_PROP_PREFIX);

        // Construct JdbcSinkConfig from FactoryConfigurations
        final Configuration config = context.getFactoryConfiguration();
        JdbcSinkConfig.Builder<?> builder = new JdbcSinkConfig.Builder<>();

        List<JdbcSinkDialectFactory<JdbcSinkConfig>> dialectFactories =
                discoverDialectFactories(getClass().getClassLoader());

        builder.connUrl(config.get(CONN_URL))
                .username(config.get(USERNAME))
                .password(config.get(PASSWORD))
                .serverTimeZone(
                        config.getOptional(SERVER_TIME_ZONE)
                                .orElse(ZoneId.systemDefault().toString()))
                .connectTimeout(config.get(CONNECT_TIMEOUT))
                .connectionPoolSize(config.get(CONNECTION_POOL_SIZE))
                .connectMaxRetries(config.get(CONNECT_MAX_RETRIES))
                .writeBatchIntervalMs(config.get(WRITE_BATCH_INTERVAL_MS))
                .writeBatchSize(config.get(WRITE_BATCH_SIZE))
                .writeMaxRetries(config.get(WRITE_MAX_RETRIES))
                .driverClassName(config.get(DRIVER_CLASS_NAME));

        Properties properties = new Properties();
        Map<String, String> jdbcProperties =
                JdbcSinkOptions.getPropertiesByPrefix(config, JDBC_PROPERTIES_PROP_PREFIX);
        properties.putAll(jdbcProperties);
        builder.jdbcProperties(properties);
        JdbcSinkConfig jdbcSinkConfig = builder.build();

        // Discover corresponding factory
        String dialect = jdbcSinkConfig.getDialect();

        List<JdbcSinkDialectFactory<JdbcSinkConfig>> availableFactories =
                dialectFactories.stream()
                        .filter(d -> dialect.equalsIgnoreCase(d.identifier()))
                        .collect(Collectors.toList());

        Preconditions.checkArgument(
                availableFactories.size() == 1,
                "There must be exactly one factory for dialect %s, but %s found.\nMatched dialects: %s\nAvailable dialects: %s",
                dialect,
                availableFactories.size(),
                availableFactories.stream().map(Object::getClass).collect(Collectors.toList()),
                dialectFactories.stream()
                        .map(JdbcSinkDialectFactory::identifier)
                        .collect(Collectors.toList()));

        JdbcSinkDialectFactory<JdbcSinkConfig> dialectFactory = availableFactories.get(0);

        if (LOG.isInfoEnabled()) {
            OptionUtils.printOptions(
                    IDENTIFIER, ConfigurationUtils.hideSensitiveValues(config.toMap()));
        }

        return new JdbcDataSink(dialectFactory.createDialect(jdbcSinkConfig), jdbcSinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CONN_URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DRIVER_CLASS_NAME);
        options.add(SERVER_TIME_ZONE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECTION_POOL_SIZE);
        options.add(CONNECT_MAX_RETRIES);
        options.add(WRITE_BATCH_INTERVAL_MS);
        options.add(WRITE_BATCH_SIZE);
        options.add(WRITE_MAX_RETRIES);
        return options;
    }

    private List<JdbcSinkDialectFactory<JdbcSinkConfig>> discoverDialectFactories(
            ClassLoader classLoader) {
        try {
            final List<JdbcSinkDialectFactory<JdbcSinkConfig>> result = new LinkedList<>();
            ServiceLoader.load(JdbcSinkDialectFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            throw new RuntimeException(
                    "Could not load service provider for JdbcSinkDialectFactory.", e);
        }
    }
}
