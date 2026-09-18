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

package org.apache.flink.cdc.connectors.sqlserver.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

/** Unit tests for {@link SqlServerSourceConfigFactory}. */
class SqlServerSourceConfigFactoryTest {

    @Test
    void testTimestampStartupOption() {
        long startupTimestampMillis = 1667232000000L;

        SqlServerSourceConfigFactory factory = new SqlServerSourceConfigFactory();
        factory.hostname("localhost")
                .port(1433)
                .databaseList("inventory")
                .tableList("inventory.dbo.products")
                .username("flinkuser")
                .password("flinkpw")
                .serverTimeZone("UTC");
        factory.startupOptions(StartupOptions.timestamp(startupTimestampMillis));

        SqlServerSourceConfig sourceConfig = factory.create(0);

        Assertions.assertThat(sourceConfig.getStartupOptions())
                .isEqualTo(StartupOptions.timestamp(startupTimestampMillis));
        Assertions.assertThat(sourceConfig.getDbzProperties().getProperty("snapshot.mode"))
                .isEqualTo("schema_only");
    }

    /**
     * Debezium 2.0 renamed {@code database.server.name} to {@code topic.prefix} and replaced {@code
     * database.dbname} with the multi-database {@code database.names}; both are required, and
     * without them the topic naming strategy cannot even be instantiated.
     */
    @Test
    void testTopicPrefixAndDatabaseNamesAreConfigured() {
        SqlServerConnectorConfig connectorConfig =
                new SqlServerConnectorConfig(newFactory().create(0).getDbzConfiguration());

        Assertions.assertThat(connectorConfig.getLogicalName())
                .isEqualTo("sqlserver_transaction_log_source");
        Assertions.assertThat(connectorConfig.getDatabaseNames()).containsExactly("inventory");
        Assertions.assertThat(
                        connectorConfig.getTopicNamingStrategy(
                                CommonConnectorConfig.TOPIC_NAMING_STRATEGY))
                .isNotNull();
    }

    /**
     * The mssql-jdbc driver shipped with Debezium 2.x defaults {@code encrypt} to true, while the
     * one shipped with Debezium 1.9 defaulted it to false. Flink CDC keeps the historical default
     * but users can still opt into TLS.
     */
    @Test
    void testEncryptDefaultsToFalseAndCanBeOverridden() {
        Assertions.assertThat(
                        newFactory().create(0).getDbzConfiguration().getString("database.encrypt"))
                .isEqualTo("false");

        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("database.encrypt", "true");
        SqlServerSourceConfigFactory factory = newFactory();
        factory.debeziumProperties(dbzProperties);
        Assertions.assertThat(factory.create(0).getDbzConfiguration().getString("database.encrypt"))
                .isEqualTo("true");
    }

    private static SqlServerSourceConfigFactory newFactory() {
        SqlServerSourceConfigFactory factory = new SqlServerSourceConfigFactory();
        factory.hostname("localhost")
                .port(1433)
                .databaseList("inventory")
                .tableList("inventory.dbo.products")
                .username("flinkuser")
                .password("flinkpw")
                .serverTimeZone("UTC");
        factory.startupOptions(StartupOptions.initial());
        return factory;
    }
}
