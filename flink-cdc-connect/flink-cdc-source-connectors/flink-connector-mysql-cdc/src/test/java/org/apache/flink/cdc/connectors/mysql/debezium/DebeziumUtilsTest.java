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

package org.apache.flink.cdc.connectors.mysql.debezium;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;

import io.debezium.connector.mysql.MySqlConnection;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Properties;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils}. */
class DebeziumUtilsTest {
    @Test
    void testCreateMySqlConnection() {
        // test without set useSSL
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("onlyTest", "test");
        MySqlSourceConfig configWithoutUseSSL = getConfig(jdbcProps);
        MySqlConnection connection0 = DebeziumUtils.createMySqlConnection(configWithoutUseSSL);
        assertJdbcUrl(
                "jdbc:mysql://localhost:3306/?useSSL=false&connectTimeout=20000&useInformationSchema=true"
                        + "&nullCatalogMeansCurrent=false&characterSetResults=UTF-8&onlyTest=test"
                        + "&zeroDateTimeBehavior=CONVERT_TO_NULL&characterEncoding=UTF-8&useUnicode=true",
                connection0.connectionString());

        // test with set useSSL=false
        jdbcProps.setProperty("useSSL", "false");
        MySqlSourceConfig configNotUseSSL = getConfig(jdbcProps);
        MySqlConnection connection1 = DebeziumUtils.createMySqlConnection(configNotUseSSL);
        assertJdbcUrl(
                "jdbc:mysql://localhost:3306/?connectTimeout=20000&useInformationSchema=true"
                        + "&nullCatalogMeansCurrent=false&characterSetResults=UTF-8&useSSL=false&onlyTest=test"
                        + "&zeroDateTimeBehavior=CONVERT_TO_NULL&characterEncoding=UTF-8&useUnicode=true",
                connection1.connectionString());

        // test with set useSSL=true
        jdbcProps.setProperty("useSSL", "true");
        MySqlSourceConfig configUseSSL = getConfig(jdbcProps);
        MySqlConnection connection2 = DebeziumUtils.createMySqlConnection(configUseSSL);
        assertJdbcUrl(
                "jdbc:mysql://localhost:3306/?connectTimeout=20000&useInformationSchema=true"
                        + "&nullCatalogMeansCurrent=false&characterSetResults=UTF-8&useSSL=true&onlyTest=test"
                        + "&zeroDateTimeBehavior=CONVERT_TO_NULL&characterEncoding=UTF-8&useUnicode=true",
                connection2.connectionString());
    }

    private MySqlSourceConfig getConfig(Properties jdbcProperties) {
        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.initial())
                .databaseList("fakeDb")
                .tableList("fakeDb.fakeTable")
                .includeSchemaChanges(false)
                .hostname("localhost")
                .port(3306)
                .splitSize(10)
                .fetchSize(2)
                .connectTimeout(Duration.ofSeconds(20))
                .username("fakeUser")
                .password("fakePw")
                .serverTimeZone(ZoneId.of("UTC").toString())
                .jdbcProperties(jdbcProperties)
                .createConfig(0);
    }

    private void assertJdbcUrl(String expected, String actual) {
        // Compare after splitting to avoid the orderless jdbc parameters in jdbc url at Java 11
        String[] expectedParam = expected.split("&");
        String[] actualParam = actual.split("&");
        Assertions.assertThat(actualParam).containsExactlyInAnyOrder(expectedParam);
    }
}
