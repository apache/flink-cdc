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

package org.apache.flink.cdc.connectors.mysql.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcPropertiesFormatter;
import org.apache.flink.cdc.connectors.jdbc.sink.JdbcDataSink;
import org.apache.flink.cdc.connectors.mysql.sink.MySqlDataSinkConfig;
import org.apache.flink.cdc.connectors.mysql.sink.MysqlPooledDataSinkFactory;
import org.apache.flink.cdc.connectors.mysql.sink.catalog.MySqlCatalog;
import org.apache.flink.cdc.connectors.mysql.sink.catalog.MySqlCatalogFactory;
import org.apache.flink.cdc.connectors.mysql.utils.OptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.connectors.mysql.sink.MySqlDataSinkOptions.*;

/** Factory for creating configured instance of {@link DataSinkFactory}. */
public class MySqlDataSinkFactory implements DataSinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlDataSinkFactory.class);

    public static final String IDENTIFIER = "mysql-writer";

    @Override
    public DataSink createDataSink(Context context) {
        final Configuration config = context.getFactoryConfiguration();
        MySqlDataSinkConfig.Builder builder = new MySqlDataSinkConfig.Builder();

        config.getOptional(HOSTNAME).ifPresent(builder::hostname);
        config.getOptional(PORT).ifPresent(builder::port);
        config.getOptional(USERNAME).ifPresent(builder::username);
        config.getOptional(PASSWORD).ifPresent(builder::password);
        builder.serverTimeZone(config.getOptional(SERVER_TIME_ZONE).orElseGet(() -> "UTC"));
        builder.connectTimeout(
                config.getOptional(CONNECT_TIMEOUT).orElseGet(() -> Duration.ofSeconds(30)));
        builder.connectionPoolSize(config.getOptional(CONNECTION_POOL_SIZE).orElseGet(() -> 20));
        builder.connectMaxRetries(config.getOptional(CONNECT_MAX_RETRIES).orElseGet(() -> 3));
        // jdbc properties
        config.getOptional(JDBC_PROPERTIES)
                .map(JdbcPropertiesFormatter::formatJdbcProperties)
                .ifPresent(builder::jdbcProperties);
        // driver class name
        builder.driverClassName(
                config.getOptional(DRIVER_CLASS_NAME).orElseGet(() -> "com.mysql.cj.jdbc.Driver"));
        // get jdbc url
        String jdbcUrl = MysqlPooledDataSinkFactory.INSTANCE.getJdbcUrl(builder.build());
        builder.connUrl(jdbcUrl);
        // print configs
        Map<String, String> map = config.toMap();
        OptionUtils.printOptions(IDENTIFIER, map);

        MySqlDataSinkConfig sinkConfig = builder.build();
        MySqlCatalog catalog = MySqlCatalogFactory.INSTANCE.createCatalog(sinkConfig);

        return new JdbcDataSink(catalog, sinkConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
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
        options.add(JDBC_PROPERTIES);

        return options;
    }
}
