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

package io.debezium.connector.oracle.logminer.logwriter;

import io.debezium.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for RAC LGWR flush JDBC URL rewriting. */
class RacCommitLogWriterFlushStrategyTest {

    @Test
    void testRewriteServiceNameJdbcUrl() {
        assertThat(
                        RacCommitLogWriterFlushStrategy.rewriteJdbcUrl(
                                "jdbc:oracle:thin:@//scan-host:1521/ORCL19", "127.0.0.1", 15210))
                .isEqualTo("jdbc:oracle:thin:@//127.0.0.1:15210/ORCL19");
    }

    @Test
    void testRewriteSidJdbcUrl() {
        assertThat(
                        RacCommitLogWriterFlushStrategy.rewriteJdbcUrl(
                                "jdbc:oracle:thin:@scan-host:1521:ORCL19", "127.0.0.1", 15210))
                .isEqualTo("jdbc:oracle:thin:@127.0.0.1:15210:ORCL19");
    }

    @Test
    void testResolveJdbcUrlFallsBackToServiceNameWhenRawUrlMissing() {
        JdbcConfiguration jdbcConfiguration =
                JdbcConfiguration.adapt(
                        JdbcConfiguration.create()
                                .with(JdbcConfiguration.HOSTNAME, "scan-host")
                                .with(JdbcConfiguration.PORT, 1521)
                                .with(JdbcConfiguration.DATABASE, "ORCL19")
                                .build());

        assertThat(
                        RacCommitLogWriterFlushStrategy.resolveJdbcUrl(
                                jdbcConfiguration, "127.0.0.1", 15210))
                .isEqualTo("jdbc:oracle:thin:@//127.0.0.1:15210/ORCL19");
    }

    @Test
    void testBuildHostJdbcConfigurationOverridesHostPortAndUrlTogether() {
        JdbcConfiguration jdbcConfiguration =
                JdbcConfiguration.adapt(
                        JdbcConfiguration.create()
                                .with(JdbcConfiguration.HOSTNAME, "scan-host")
                                .with(JdbcConfiguration.PORT, 1521)
                                .with(JdbcConfiguration.DATABASE, "ORCL19")
                                .with("url", "jdbc:oracle:thin:@//scan-host:1521/ORCL19")
                                .build());

        JdbcConfiguration hostConfig =
                RacCommitLogWriterFlushStrategy.buildHostJdbcConfiguration(
                        jdbcConfiguration, "127.0.0.1", 15210);

        assertThat(hostConfig.getHostname()).isEqualTo("127.0.0.1");
        assertThat(hostConfig.getPort()).isEqualTo(15210);
        assertThat(hostConfig.getString("url"))
                .isEqualTo("jdbc:oracle:thin:@//127.0.0.1:15210/ORCL19");
    }

    @Test
    void testBuildHostJdbcConfigurationRestoresCredentialsWhenRuntimeConfigMissesThem() {
        JdbcConfiguration runtimeJdbcConfiguration =
                JdbcConfiguration.adapt(
                        JdbcConfiguration.create()
                                .with(JdbcConfiguration.HOSTNAME, "scan-host")
                                .with(JdbcConfiguration.PORT, 1521)
                                .build());

        JdbcConfiguration hostConfig =
                RacCommitLogWriterFlushStrategy.buildHostJdbcConfiguration(
                        runtimeJdbcConfiguration,
                        "jdbc:oracle:thin:@//scan-host:1521/ORCL19",
                        "ORCL19",
                        "flinkuser",
                        "flinkpw",
                        "127.0.0.1",
                        15210);

        assertThat(hostConfig.getUser()).isEqualTo("flinkuser");
        assertThat(hostConfig.getPassword()).isEqualTo("flinkpw");
        assertThat(hostConfig.getString("url"))
                .isEqualTo("jdbc:oracle:thin:@//127.0.0.1:15210/ORCL19");
    }
}
