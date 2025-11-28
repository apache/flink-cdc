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

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for MySQL binlog reconnection configuration. */
class MySqlBinlogReconnectionConfigTest {

    @Test
    void testDefaultReconnectionConfig() {
        MySqlSourceConfig config = createDefaultConfig();

        assertThat(config.isBinlogFailOnReconnectionError()).isFalse();
        assertThat(config.getBinlogReconnectionMaxRetries()).isEqualTo(3);
        assertThat(config.getBinlogReconnectionTimeout()).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void testCustomReconnectionConfigViaConfigFactory() {
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("test_user")
                        .password("test_password")
                        .databaseList("test_db")
                        .tableList("test_db.test_table")
                        .binlogFailOnReconnectionError(true)
                        .binlogReconnectionMaxRetries(5)
                        .binlogReconnectionTimeout(Duration.ofMinutes(10))
                        .createConfig(0);

        assertThat(config.isBinlogFailOnReconnectionError()).isTrue();
        assertThat(config.getBinlogReconnectionMaxRetries()).isEqualTo(5);
        assertThat(config.getBinlogReconnectionTimeout()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void testReconnectionConfigViaDebeziumProperties() {
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("binlog.fail-on-reconnection-error", "true");
        debeziumProps.setProperty("binlog.reconnection.max-retries", "7");
        debeziumProps.setProperty("binlog.reconnection.timeout", "900000"); // 15 minutes in ms

        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("test_user")
                        .password("test_password")
                        .databaseList("test_db")
                        .tableList("test_db.test_table")
                        .debeziumProperties(debeziumProps)
                        .createConfig(0);

        // Verify configuration is accessible through MySqlConnectorConfig
        MySqlConnectorConfig connectorConfig = config.getMySqlConnectorConfig();
        Configuration configuration = connectorConfig.getConfig();

        assertThat(
                        configuration.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), false))
                .isTrue();
        assertThat(
                        configuration.getInteger(
                                MySqlSourceOptions.BINLOG_RECONNECTION_MAX_RETRIES.key(), 3))
                .isEqualTo(7);
        assertThat(
                        configuration.getLong(
                                MySqlSourceOptions.BINLOG_RECONNECTION_TIMEOUT.key(), 300000L))
                .isEqualTo(900000L);
    }

    @Test
    void testDebeziumPropertiesOverrideDefaults() {
        // User provides custom values via debezium properties
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("binlog.fail-on-reconnection-error", "true");
        debeziumProps.setProperty("binlog.reconnection.max-retries", "2");
        debeziumProps.setProperty("binlog.reconnection.timeout", "60000"); // 1 minute

        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("test_user")
                        .password("test_password")
                        .databaseList("test_db")
                        .tableList("test_db.test_table")
                        // Also set via config factory (should be overridden by debezium properties)
                        .binlogFailOnReconnectionError(false)
                        .binlogReconnectionMaxRetries(10)
                        .binlogReconnectionTimeout(Duration.ofMinutes(20))
                        .debeziumProperties(debeziumProps)
                        .createConfig(0);

        MySqlConnectorConfig connectorConfig = config.getMySqlConnectorConfig();
        Configuration configuration = connectorConfig.getConfig();

        // Debezium properties should win
        assertThat(
                        configuration.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), false))
                .isTrue();
        assertThat(
                        configuration.getInteger(
                                MySqlSourceOptions.BINLOG_RECONNECTION_MAX_RETRIES.key(), 3))
                .isEqualTo(2);
        assertThat(
                        configuration.getLong(
                                MySqlSourceOptions.BINLOG_RECONNECTION_TIMEOUT.key(), 300000L))
                .isEqualTo(60000L);
    }

    @Test
    void testConfigFactoryValuesUsedWhenDebeziumPropertiesNotProvided() {
        // Set custom values via config factory, no debezium properties
        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("test_user")
                        .password("test_password")
                        .databaseList("test_db")
                        .tableList("test_db.test_table")
                        .binlogFailOnReconnectionError(true)
                        .binlogReconnectionMaxRetries(8)
                        .binlogReconnectionTimeout(Duration.ofMinutes(12))
                        .createConfig(0);

        MySqlConnectorConfig connectorConfig = config.getMySqlConnectorConfig();
        Configuration configuration = connectorConfig.getConfig();

        // Config factory values should be used as defaults
        assertThat(
                        configuration.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), false))
                .isTrue();
        assertThat(
                        configuration.getInteger(
                                MySqlSourceOptions.BINLOG_RECONNECTION_MAX_RETRIES.key(), 3))
                .isEqualTo(8);
        assertThat(
                        configuration.getLong(
                                MySqlSourceOptions.BINLOG_RECONNECTION_TIMEOUT.key(), 300000L))
                .isEqualTo(720000L); // 12 minutes in ms
    }

    @Test
    void testPartialDebeziumPropertiesConfiguration() {
        // User only provides some of the properties
        Properties debeziumProps = new Properties();
        debeziumProps.setProperty("binlog.fail-on-reconnection-error", "true");
        // Not setting max-retries and timeout - should use defaults

        MySqlSourceConfig config =
                new MySqlSourceConfigFactory()
                        .hostname("localhost")
                        .port(3306)
                        .username("test_user")
                        .password("test_password")
                        .databaseList("test_db")
                        .tableList("test_db.test_table")
                        .debeziumProperties(debeziumProps)
                        .createConfig(0);

        MySqlConnectorConfig connectorConfig = config.getMySqlConnectorConfig();
        Configuration configuration = connectorConfig.getConfig();

        assertThat(
                        configuration.getBoolean(
                                MySqlSourceOptions.BINLOG_FAIL_ON_RECONNECTION_ERROR.key(), false))
                .isTrue();
        // Should use MySqlSourceConfigFactory defaults for unspecified properties
        assertThat(
                        configuration.getInteger(
                                MySqlSourceOptions.BINLOG_RECONNECTION_MAX_RETRIES.key(), 999))
                .isEqualTo(3);
        assertThat(
                        configuration.getLong(
                                MySqlSourceOptions.BINLOG_RECONNECTION_TIMEOUT.key(), 999999L))
                .isEqualTo(300000L);
    }

    private MySqlSourceConfig createDefaultConfig() {
        return new MySqlSourceConfigFactory()
                .hostname("localhost")
                .port(3306)
                .username("test_user")
                .password("test_password")
                .databaseList("test_db")
                .tableList("test_db.test_table")
                .createConfig(0);
    }
}
