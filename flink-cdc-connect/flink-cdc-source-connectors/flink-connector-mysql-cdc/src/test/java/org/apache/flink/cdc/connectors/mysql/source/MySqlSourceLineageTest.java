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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for MySQL source lineage. */
class MySqlSourceLineageTest {

    @Test
    void testGetLineageVertexFallsBackToConnectorDatasetWithoutTables() {
        MySqlSource<String> source =
                new MySqlSource<>(
                        configFactory(StartupOptions.initial()),
                        null,
                        (metrics, sourceConfig) -> null);

        SourceLineageVertex vertex = (SourceLineageVertex) source.getLineageVertex();

        assertThat(vertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(vertex.datasets()).extracting(LineageDataset::name).containsExactly("mysql");
        assertThat(vertex.datasets().get(0).namespace()).isEqualTo("mysql://mysql-host:3307");
        assertThat(vertex.datasets().get(0).facets()).doesNotContainKey("schema");
    }

    @Test
    void testGetLineageVertexUsesBoundednessFromStartupOptions() {
        MySqlSource<String> source =
                new MySqlSource<>(
                        configFactory(StartupOptions.snapshot()),
                        null,
                        (metrics, sourceConfig) -> null);

        SourceLineageVertex vertex = (SourceLineageVertex) source.getLineageVertex();

        assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    private MySqlSourceConfigFactory configFactory(StartupOptions startupOptions) {
        return new MySqlSourceConfigFactory()
                .startupOptions(startupOptions)
                .databaseList("db")
                .tableList("db.customers")
                .includeSchemaChanges(false)
                .hostname("mysql-host")
                .port(3307)
                .splitSize(10)
                .fetchSize(2)
                .connectTimeout(Duration.ofSeconds(20))
                .username("user")
                .password("password")
                .serverTimeZone(ZoneId.of("UTC").toString());
    }
}
