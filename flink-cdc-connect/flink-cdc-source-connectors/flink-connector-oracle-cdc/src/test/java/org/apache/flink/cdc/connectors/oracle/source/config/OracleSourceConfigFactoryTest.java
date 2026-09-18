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

package org.apache.flink.cdc.connectors.oracle.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleSourceConfigFactory}. */
class OracleSourceConfigFactoryTest {

    /**
     * Debezium 2.0 renamed {@code database.server.name} to {@code topic.prefix}. Without it the
     * topic naming strategy cannot be built, which breaks every fetch task.
     */
    @Test
    void testTopicPrefixIsConfigured() {
        OracleSourceConfigFactory factory = new OracleSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(1521);
        factory.username("dbzuser");
        factory.password("dbz");
        factory.databaseList("ORCLCDB");
        factory.schemaList("DEBEZIUM");
        factory.tableList("ORCLCDB.DEBEZIUM.PRODUCTS");
        factory.startupOptions(StartupOptions.initial());

        OracleConnectorConfig connectorConfig =
                new OracleConnectorConfig(factory.create(0).getDbzConfiguration());

        assertThat(connectorConfig.getLogicalName()).isEqualTo("oracle_logminer");
        assertThat(
                        connectorConfig.getTopicNamingStrategy(
                                CommonConnectorConfig.TOPIC_NAMING_STRATEGY))
                .isNotNull();
    }
}
