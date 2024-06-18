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

package org.apache.flink.cdc.connectors.jdbc.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link JdbcSinkConfig}. */
class JdbcSinkConfigTest {
    private static final String TEST_HOSTNAME = "localhost";
    private static final int TEST_PORT = 5432;
    private static final String TEST_CONN_URL = "jdbc:mysql://" + TEST_HOSTNAME + ":" + TEST_PORT;
    private static final String TEST_USERNAME = "admin";
    private static final String TEST_PASSWORD = "password";
    private static final String TEST_TABLE = "testtable";
    private static final String TEST_DRIVER_CLASS_NAME = "org.postgresql.Driver";
    private static final String TEST_SERVER_TIME_ZONE = "UTC";
    private static final Duration TEST_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final int TEST_CONNECT_MAX_RETRIES = 3;
    private static final int TEST_CONNECTION_POOL_SIZE = 5;
    private static final long TEST_WRITE_INTERVAL_MS = 31415926;
    private static final int TEST_WRITE_BATCH_SIZE = 17;
    private static final int TEST_WRITE_MAX_RETRIES = 5;

    private JdbcSinkConfig.Builder<?> builder;

    @BeforeEach
    void setUp() {
        builder =
                new JdbcSinkConfig.Builder<>()
                        .connUrl(TEST_CONN_URL)
                        .username(TEST_USERNAME)
                        .password(TEST_PASSWORD)
                        .table(TEST_TABLE)
                        .driverClassName(TEST_DRIVER_CLASS_NAME)
                        .serverTimeZone(TEST_SERVER_TIME_ZONE)
                        .connectTimeout(TEST_CONNECT_TIMEOUT)
                        .connectMaxRetries(TEST_CONNECT_MAX_RETRIES)
                        .connectionPoolSize(TEST_CONNECTION_POOL_SIZE)
                        .writeBatchIntervalMs(TEST_WRITE_INTERVAL_MS)
                        .writeBatchSize(TEST_WRITE_BATCH_SIZE)
                        .writeMaxRetries(TEST_WRITE_MAX_RETRIES);
    }

    @Test
    void testJdbcSinkConfigBuilder() {
        JdbcSinkConfig config = builder.build();

        assertThat(config.getConnUrl()).isEqualTo(TEST_CONN_URL);
        assertThat(config.getUsername()).isEqualTo(TEST_USERNAME);
        assertThat(config.getPassword()).isEqualTo(TEST_PASSWORD);
        assertThat(config.getTable()).isEqualTo(TEST_TABLE);
        assertThat(config.getDriverClassName()).isEqualTo(TEST_DRIVER_CLASS_NAME);
        assertThat(config.getServerTimeZone()).isEqualTo(TEST_SERVER_TIME_ZONE);
        assertThat(config.getConnectTimeout()).isEqualTo(TEST_CONNECT_TIMEOUT);
        assertThat(config.getConnectMaxRetries()).isEqualTo(TEST_CONNECT_MAX_RETRIES);
        assertThat(config.getConnectionPoolSize()).isEqualTo(TEST_CONNECTION_POOL_SIZE);
        assertThat(config.getWriteBatchIntervalMs()).isEqualTo(TEST_WRITE_INTERVAL_MS);
        assertThat(config.getWriteBatchSize()).isEqualTo(TEST_WRITE_BATCH_SIZE);
        assertThat(config.getWriteMaxRetries()).isEqualTo(TEST_WRITE_MAX_RETRIES);
    }

    @Test
    void testJdbcSinkConfigProperties() {
        Properties properties = new Properties();
        properties.setProperty("key", "value");

        JdbcSinkConfig config = builder.jdbcProperties(properties).build();

        assertThat(config.getJdbcProperties()).isEqualTo(properties);
    }
}
