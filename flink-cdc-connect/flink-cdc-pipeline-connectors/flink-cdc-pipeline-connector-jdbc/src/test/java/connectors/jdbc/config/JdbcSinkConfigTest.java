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

package connectors.jdbc.config;

import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test class for {@link JdbcSinkConfig}. */
class JdbcSinkConfigTest {
    private static final String TEST_HOSTNAME = "localhost";
    private static final int TEST_PORT = 5432;
    private static final String TEST_USERNAME = "admin";
    private static final String TEST_PASSWORD = "password";
    private static final String TEST_DATABASE = "testdb";
    private static final String TEST_SCHEMA = "testschema";
    private static final String TEST_TABLE = "testtable";
    private static final String TEST_DRIVER_CLASS_NAME = "org.postgresql.Driver";
    private static final String TEST_SERVER_TIME_ZONE = "UTC";
    private static final Duration TEST_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final int TEST_CONNECT_MAX_RETRIES = 3;
    private static final int TEST_CONNECTION_POOL_SIZE = 5;

    private JdbcSinkConfig.Builder builder;

    @BeforeEach
    void setUp() {
        builder =
                new JdbcSinkConfig.Builder()
                        .hostname(TEST_HOSTNAME)
                        .port(TEST_PORT)
                        .username(TEST_USERNAME)
                        .password(TEST_PASSWORD)
                        .table(TEST_TABLE)
                        .driverClassName(TEST_DRIVER_CLASS_NAME)
                        .serverTimeZone(TEST_SERVER_TIME_ZONE)
                        .connectTimeout(TEST_CONNECT_TIMEOUT)
                        .connectMaxRetries(TEST_CONNECT_MAX_RETRIES)
                        .connectionPoolSize(TEST_CONNECTION_POOL_SIZE);
    }

    @Test
    void testJdbcSinkConfigBuilder() {
        JdbcSinkConfig config = builder.build();

        assertEquals(TEST_HOSTNAME, config.getHostname());
        assertEquals(TEST_PORT, config.getPort());
        assertEquals(TEST_USERNAME, config.getUsername());
        assertEquals(TEST_PASSWORD, config.getPassword());
        assertEquals(TEST_TABLE, config.getTable());
        assertEquals(TEST_DRIVER_CLASS_NAME, config.getDriverClassName());
        assertEquals(TEST_SERVER_TIME_ZONE, config.getServerTimeZone());
        assertEquals(TEST_CONNECT_TIMEOUT, config.getConnectTimeout());
        assertEquals(TEST_CONNECT_MAX_RETRIES, config.getConnectMaxRetries());
        assertEquals(TEST_CONNECTION_POOL_SIZE, config.getConnectionPoolSize());
    }

    @Test
    void testJdbcSinkConfigProperties() {
        Properties properties = new Properties();
        properties.setProperty("key", "value");

        JdbcSinkConfig config = builder.jdbcProperties(properties).build();

        assertEquals(properties, config.getJdbcProperties());
    }
}
