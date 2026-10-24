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

package org.apache.flink.cdc.common.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LineageUtils}. */
class LineageUtilsTest {

    @Test
    void testSourceLineageVertexWithTablesAndSchemas() {
        LinkedHashMap<String, String> tableSchema = new LinkedHashMap<>();
        tableSchema.put("id", "int NOT NULL");
        tableSchema.put("name", "varchar(255)");
        Map<String, LinkedHashMap<String, String>> tableSchemas =
                java.util.Collections.singletonMap("db.customers", tableSchema);

        SourceLineageVertex vertex =
                LineageUtils.sourceLineageVertex(
                        "mysql",
                        "mysql-host",
                        3306,
                        false,
                        Arrays.asList("db.customers", "db.orders"),
                        tableSchemas);

        assertThat(vertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(vertex.datasets())
                .extracting(LineageDataset::name)
                .containsExactly("db.customers", "db.orders");

        LineageDataset customers = vertex.datasets().get(0);
        assertThat(customers.namespace()).isEqualTo("mysql://mysql-host:3306");

        DatasetConfigFacet configFacet = (DatasetConfigFacet) customers.facets().get("config");
        assertThat(configFacet.config()).containsEntry("type", "mysql-cdc");

        DatasetSchemaFacet schemaFacet = (DatasetSchemaFacet) customers.facets().get("schema");
        assertThat(schemaFacet.fields().get("id").type()).isEqualTo("int NOT NULL");
        assertThat(schemaFacet.fields().get("name").type()).isEqualTo("varchar(255)");

        assertThat(vertex.datasets().get(1).facets()).doesNotContainKey("schema");
    }

    @Test
    void testSourceLineageVertexWithoutTablesUsesConnectorDataset() {
        SourceLineageVertex vertex =
                LineageUtils.sourceLineageVertex(
                        "mysql", "mysql-host", 3306, true, java.util.Collections.emptyList());

        assertThat(vertex.boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(vertex.datasets()).hasSize(1);
        assertThat(vertex.datasets().get(0).name()).isEqualTo("mysql");
        assertThat(vertex.datasets().get(0).namespace()).isEqualTo("mysql://mysql-host:3306");
    }
}
