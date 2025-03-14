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

package org.apache.flink.cdc.connectors.mysql.sink.confg;

import org.apache.flink.cdc.connectors.mysql.sink.MySqlDataSinkConfig;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link MySqlDataSinkConfig}. */
public class MySqlDataSinkConfigTest {

    /** Test the creation of MySqlDataSinkConfig using the Builder. */
    @Test
    public void testMySqlDataSinkConfigCreation() {
        Properties props = new Properties();
        props.setProperty("useSSL", "true");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlDataSinkConfig config =
                new MySqlDataSinkConfig.Builder()
                        .hostname("localhost")
                        .port(3306)
                        .username("root")
                        .password("password")
                        .table("mytable")
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .serverTimeZone("UTC")
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(10)
                        .jdbcProperties(props)
                        .build();

        assertThat(config.getHostname()).isEqualTo("localhost");
        assertThat(config.getPort()).isEqualTo(3306);
        assertThat(config.getUsername()).isEqualTo("root");
        assertThat(config.getPassword()).isEqualTo("password");
        assertThat(config.getTable()).isEqualTo("mytable");
        assertThat(config.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
        assertThat(config.getServerTimeZone()).isEqualTo("UTC");
        assertThat(config.getConnectTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.getConnectMaxRetries()).isEqualTo(3);
        assertThat(config.getConnectionPoolSize()).isEqualTo(10);
        assertThat(config.getJdbcProperties().getProperty("useSSL")).isEqualTo("true");
        assertThat(config.getJdbcProperties().getProperty("allowPublicKeyRetrieval"))
                .isEqualTo("true");
    }
}
