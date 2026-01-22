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

package org.apache.flink.cdc.connectors.mysql.source.config;

import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for MySQL reconnection configuration in {@link MySqlSourceConfig}. */
class MySqlReconnectionConfigTest {

    @Test
    void testReconnectionConfigurationExtraction() {
        // Create MySqlSourceConfig with specific reconnection values
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .connectTimeout(Duration.ofSeconds(15))
                        .connectMaxRetries(5)
                        .createConfig(0);

        // Verify the configuration values are correctly set in the MySqlSourceConfig
        assertThat(config.getConnectTimeout()).isEqualTo(Duration.ofSeconds(15));
        assertThat(config.getConnectMaxRetries()).isEqualTo(5);
        assertThat(config.isBinlogFailOnReconnectionError()).isFalse();

        // Verify the configuration is properly propagated to Debezium configuration
        Configuration debeziumConfig = config.getDbzConfiguration();

        // Check that Debezium gets the correct timeout value in milliseconds
        assertThat(debeziumConfig.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS))
                .isEqualTo(15000L);
        assertThat(
                        debeziumConfig.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key()))
                .isFalse();
    }

    @Test
    void testDefaultReconnectionConfiguration() {
        // Create MySqlSourceConfig with default values
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .createConfig(0);

        // Verify default values are applied
        assertThat(config.getConnectTimeout())
                .isEqualTo(MySqlSourceOptions.CONNECT_TIMEOUT.defaultValue());
        assertThat(config.getConnectMaxRetries())
                .isEqualTo(MySqlSourceOptions.CONNECT_MAX_RETRIES.defaultValue());
        assertThat(config.isBinlogFailOnReconnectionError())
                .isEqualTo(MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.defaultValue());

        Configuration debeziumConfig = config.getDbzConfiguration();

        // Verify defaults are propagated to Debezium
        assertThat(debeziumConfig.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS))
                .isEqualTo(MySqlSourceOptions.CONNECT_TIMEOUT.defaultValue().toMillis());
        assertThat(
                        debeziumConfig.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key()))
                .isEqualTo(MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.defaultValue());
    }

    @Test
    void testCustomReconnectionTimeoutRange() {
        // Test minimum timeout
        MySqlSourceConfig minConfig =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .connectTimeout(Duration.ofMillis(250)) // Minimum allowed by MySQL
                        .createConfig(0);

        assertThat(minConfig.getConnectTimeout()).isEqualTo(Duration.ofMillis(250));

        // Test large timeout
        MySqlSourceConfig maxConfig =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .connectTimeout(Duration.ofMinutes(5))
                        .createConfig(0);

        assertThat(maxConfig.getConnectTimeout()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void testReconnectionConfigurationConsistency() {
        Duration customTimeout = Duration.ofSeconds(20);
        int customRetries = 7;
        boolean customFailOnError = true;

        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .connectTimeout(customTimeout)
                        .connectMaxRetries(customRetries)
                        .binlogFailOnReconnectionError(customFailOnError)
                        .createConfig(0);

        // Ensure all custom values are consistently applied
        assertThat(config.getConnectTimeout()).isEqualTo(customTimeout);
        assertThat(config.getConnectMaxRetries()).isEqualTo(customRetries);
        assertThat(config.isBinlogFailOnReconnectionError()).isEqualTo(customFailOnError);

        // Verify consistency between MySqlSourceConfig and Debezium configuration
        Configuration debeziumConfig = config.getDbzConfiguration();

        assertThat(debeziumConfig.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS))
                .isEqualTo(customTimeout.toMillis());
        assertThat(
                        debeziumConfig.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key()))
                .isEqualTo(customFailOnError);
    }

    @Test
    void testReconnectionConfigurationFromProperties() {
        Properties props = new Properties();
        props.setProperty(MySqlSourceOptions.CONNECT_TIMEOUT.key(), "PT25S");
        props.setProperty(MySqlSourceOptions.CONNECT_MAX_RETRIES.key(), "10");
        props.setProperty(MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), "true");

        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .debeziumProperties(props)
                        .createConfig(0);

        Configuration debeziumConfig = config.getDbzConfiguration();

        // Verify properties are correctly read and applied
        assertThat(debeziumConfig.getString(MySqlSourceOptions.CONNECT_TIMEOUT.key()))
                .isEqualTo("PT25S");
        assertThat(debeziumConfig.getInteger(MySqlSourceOptions.CONNECT_MAX_RETRIES.key()))
                .isEqualTo(10);
        assertThat(
                        debeziumConfig.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key()))
                .isTrue();
    }

    @Test
    void testBinlogFailOnReconnectionErrorFalseAllowsInfiniteRetries() {
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .databaseList("test_db")
                        .tableList("test_db.products")
                        .startupOptions(StartupOptions.initial())
                        .connectMaxRetries(3) // Should be ignored when failOnError is false
                        .binlogFailOnReconnectionError(false) // Allow infinite retries
                        .createConfig(0);

        // When binlogFailOnReconnectionError is false, it should allow infinite retries
        assertThat(config.isBinlogFailOnReconnectionError()).isFalse();
        assertThat(config.getConnectMaxRetries()).isEqualTo(3);

        Configuration debeziumConfig = config.getDbzConfiguration();
        assertThat(
                        debeziumConfig.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key()))
                .isFalse();
    }
}
