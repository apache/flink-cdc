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

package org.apache.flink.cdc.connectors.postgres.source.config;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSourceConfig}. */
class PostgresSourceConfigTest {

    @Test
    void testTableFilterIsCaseSensitiveAndPreservesRegex() {
        Tables.TableFilter tableFilter =
                createSourceConfig().getTableFilters().dataCollectionFilter();

        assertThat(tableFilter.isIncluded(new TableId(null, "public", "t_order"))).isTrue();
        assertThat(tableFilter.isIncluded(new TableId(null, "public", "T_ORDER"))).isFalse();
    }

    @Test
    void testTableFilterCanBeReappliedAfterConnectorConfigReconstruction() {
        PostgresSourceConfig sourceConfig = createSourceConfig();
        PostgresConnectorConfig reconstructedConfig =
                new PostgresConnectorConfig(sourceConfig.getDbzConnectorConfig().getConfig());

        Tables.TableFilter tableFilter =
                sourceConfig
                        .applyCaseSensitiveTableFilter(reconstructedConfig)
                        .getTableFilters()
                        .dataCollectionFilter();

        assertThat(tableFilter.isIncluded(new TableId(null, "public", "t_order"))).isTrue();
        assertThat(tableFilter.isIncluded(new TableId(null, "public", "T_ORDER"))).isFalse();
    }

    private PostgresSourceConfig createSourceConfig() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"public"});
        configFactory.tableList("public\\.t_.*");
        configFactory.username("user");
        configFactory.password("password");

        return configFactory.create(0);
    }
}
