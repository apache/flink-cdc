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

package org.apache.flink.cdc.connectors.db2.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.db2.Db2ConnectorConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Db2SourceConfigFactory}. */
class Db2SourceConfigFactoryTest {

    /**
     * Debezium 2.0 renamed {@code database.server.name} to {@code topic.prefix}. Without it the
     * connector configuration does not validate and the topic naming strategy cannot be built.
     */
    @Test
    void testTopicPrefixIsConfigured() {
        Db2SourceConfigFactory factory = new Db2SourceConfigFactory();
        factory.hostname("localhost");
        factory.port(50000);
        factory.username("db2inst1");
        factory.password("flinkpw");
        factory.databaseList("testdb");
        factory.tableList("testdb.DB2INST1.PRODUCTS");
        factory.startupOptions(StartupOptions.initial());

        Db2ConnectorConfig connectorConfig =
                new Db2ConnectorConfig(factory.create(0).getDbzConfiguration());

        assertThat(connectorConfig.getLogicalName()).isEqualTo("Db2_transaction_log_source");
        assertThat(
                        connectorConfig.getTopicNamingStrategy(
                                CommonConnectorConfig.TOPIC_NAMING_STRATEGY))
                .isNotNull();
    }
}
