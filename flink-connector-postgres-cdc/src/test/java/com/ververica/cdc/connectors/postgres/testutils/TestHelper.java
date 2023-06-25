/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.postgres.testutils;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/** A helper class for testing. */
public class TestHelper {

    private static final String TEST_SERVER = "test_server";

    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    private static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "postgres")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 5432)
                .withDefault(JdbcConfiguration.USER, "postgres")
                .withDefault(JdbcConfiguration.PASSWORD, "postgres")
                .with(PostgresConnectorConfig.MAX_RETRIES, 2)
                .with(PostgresConnectorConfig.RETRY_DELAY_MS, 2000)
                .build();
    }

    /**
     * Returns a default configuration for the PostgreSQL connector. Modified from Debezium
     * project's postgres TestHelper.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((field, value) -> builder.with("database." + field, value));
        builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, TEST_SERVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true)
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 100)
                .with(PostgresConnectorConfig.PLUGIN_NAME, "decoderbufs")
                .with(
                        PostgresConnectorConfig.SSL_MODE,
                        PostgresConnectorConfig.SecureConnectionMode.DISABLED);
        final String testNetworkTimeout =
                System.getProperty(TEST_PROPERTY_PREFIX + "network.timeout");
        if (testNetworkTimeout != null && testNetworkTimeout.length() != 0) {
            builder.with(
                    PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS,
                    Integer.parseInt(testNetworkTimeout));
        }
        return builder;
    }
}
