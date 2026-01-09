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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PostgresPartitionRules}. */
class PostgresPartitionRulesTest {

    @Test
    void testParseColonFormat() {
        String tables = "public.orders";
        String partitionTables = "public.orders:public.orders_\\d+";

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        assertThat(rules.hasPartitionRules()).isTrue();
        assertThat(rules.getConfiguredParents()).hasSize(1);
        assertThat(rules.getChildToParentMappings()).hasSize(1);
    }

    @Test
    void testParseWithoutColonThrowsException() {
        String tables = "public.orders,public.products";
        String partitionTables = "public.orders_\\d+";

        assertThatThrownBy(() -> PostgresPartitionRules.parse(tables, partitionTables, "public"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must use colon format");
    }

    @Test
    void testEmptyPartitionTables() {
        PostgresPartitionRules rules =
                PostgresPartitionRules.parse("public.orders", null, "public");

        assertThat(rules.hasPartitionRules()).isFalse();
        assertThat(rules.getConfiguredParents()).isEmpty();
        assertThat(rules.getChildToParentMappings()).isEmpty();
    }

    @Test
    void testParseColonFormatDetailed() {
        String tables = "public.orders,public.products_by_category";
        String partitionTables =
                "public.orders:public\\.orders_.*,public.products:public\\.products_.*";

        PostgresPartitionRules rules = PostgresPartitionRules.parse(tables, partitionTables, null);

        assertThat(rules.hasPartitionRules()).isTrue();
        // configuredParents includes:
        // - explicit from colon left side: public.orders, public.products
        // - derived from child patterns: public.orders, public.products
        // Total unique: 2
        assertThat(rules.getConfiguredParents()).hasSize(2);
        assertThat(rules.getChildToParentMappings()).hasSize(2);
    }

    @Test
    void testNormalizePatternIgnoreCatalogWithCatalog() {
        String pattern = "mydb.public.orders_\\d+";
        String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog(pattern);

        assertThat(normalized).isEqualTo("public.orders_\\d+");
    }

    @Test
    void testNormalizePatternIgnoreCatalogWithoutCatalog() {
        String pattern = "public.orders_\\d+";
        String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog(pattern);

        assertThat(normalized).isEqualTo("public.orders_\\d+");
    }

    @Test
    void testNormalizePatternIgnoreCatalogWithEscapedDots() {
        String pattern = "catalog\\.public\\.orders_\\d+";
        String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog(pattern);

        assertThat(normalized).isEqualTo("public\\.orders_\\d+");
    }

    @Test
    void testNormalizePatternIgnoreCatalogWithRegexQuantifier() {
        String pattern = "mydb.public.orders.*";
        String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog(pattern);

        assertThat(normalized).isEqualTo("public.orders.*");
    }

    @Test
    void testNormalizePatternIgnoreCatalogWithNullInput() {
        String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog(null);

        assertThat(normalized).isNull();
    }

    @Test
    void testNormalizePatternIgnoreCatalogWithEmptyInput() {
        String normalized = PostgresPartitionRules.normalizePatternIgnoreCatalog("");

        assertThat(normalized).isEmpty();
    }

    @Test
    void testExtractDefaultSchemaFromConfigWithSingleSchema() {
        String schemaList = "public";
        String defaultSchema = PostgresPartitionRules.extractDefaultSchemaFromConfig(schemaList);

        assertThat(defaultSchema).isEqualTo("public");
    }

    @Test
    void testExtractDefaultSchemaFromConfigWithMultipleSchemas() {
        String schemaList = "public,my_schema,another_schema";
        String defaultSchema = PostgresPartitionRules.extractDefaultSchemaFromConfig(schemaList);

        assertThat(defaultSchema).isEqualTo("public");
    }

    @Test
    void testExtractDefaultSchemaFromConfigWithNullInput() {
        String defaultSchema = PostgresPartitionRules.extractDefaultSchemaFromConfig(null);

        assertThat(defaultSchema).isEqualTo("public");
    }

    @Test
    void testExtractDefaultSchemaFromConfigWithEmptyInput() {
        String defaultSchema = PostgresPartitionRules.extractDefaultSchemaFromConfig("");

        assertThat(defaultSchema).isEqualTo("public");
    }

    @Test
    void testCreateTableId() {
        TableId tableId = PostgresPartitionRules.createTableId("public", "orders");

        assertThat(tableId.catalog()).isNull();
        assertThat(tableId.schema()).isEqualTo("public");
        assertThat(tableId.table()).isEqualTo("orders");
    }

    @Test
    void testCreateTableIdWithNullSchema() {
        TableId tableId = PostgresPartitionRules.createTableId(null, "orders");

        assertThat(tableId.catalog()).isNull();
        assertThat(tableId.schema()).isNull();
        assertThat(tableId.table()).isEqualTo("orders");
    }

    @Test
    void testGetSchemaTableName() {
        TableId tableId = new TableId("mydb", "public", "orders");
        String schemaTable = PostgresPartitionRules.getSchemaTableName(tableId);

        assertThat(schemaTable).isEqualTo("public.orders");
    }

    @Test
    void testGetSchemaTableNameWithNullInput() {
        String schemaTable = PostgresPartitionRules.getSchemaTableName(null);

        assertThat(schemaTable).isNull();
    }

    @Test
    void testToComparableTableId() {
        TableId original = new TableId("mydb", "public", "orders");
        TableId comparable = PostgresPartitionRules.toComparableTableId(original);

        assertThat(comparable.catalog()).isNull();
        assertThat(comparable.schema()).isEqualTo("public");
        assertThat(comparable.table()).isEqualTo("orders");
    }

    @Test
    void testToComparableTableIdWithNullInput() {
        TableId comparable = PostgresPartitionRules.toComparableTableId(null);

        assertThat(comparable).isNull();
    }

    @Test
    void testParseWithComplexPattern() {
        String tables = "public.orders";
        String partitionTables = "public.orders:public\\.orders_\\d{4}_\\d{2}";

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        assertThat(rules.hasPartitionRules()).isTrue();
        assertThat(rules.getConfiguredParents()).hasSize(1);
        assertThat(rules.getChildToParentMappings()).hasSize(1);

        Set<TableId> parents = rules.getConfiguredParents();
        TableId parent = parents.iterator().next();
        assertThat(parent.schema()).isEqualTo("public");
        assertThat(parent.table()).isEqualTo("orders");
    }

    @Test
    void testParseWithMultipleParentsAndChildren() {
        String tables = "public.orders,public.products,public.customers";
        String partitionTables =
                "public.orders:public\\.orders_\\d+,public.products:public\\.products_\\d+,public.customers:public\\.customers_\\d+";

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        assertThat(rules.hasPartitionRules()).isTrue();
        assertThat(rules.getConfiguredParents()).hasSize(3);
        assertThat(rules.getChildToParentMappings()).hasSize(3);
    }

    @Test
    void testGetChildTablePatterns() {
        String tables = "public.orders";
        String partitionTables = "public.orders:public.orders_\\d+";

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        assertThat(rules.getChildTablePatterns()).isNotEmpty();
    }

    @Test
    void testGetTablesPattern() {
        String tables = "public.orders,public.products";
        String partitionTables = "public.orders:public.orders_\\d+";

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        assertThat(rules.getTablesPattern()).isEqualTo(tables);
    }

    @Test
    void testGetPartitionTablesPattern() {
        String tables = "public.orders";
        String partitionTables = "public.orders:public.orders_\\d+";

        PostgresPartitionRules rules =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        assertThat(rules.getPartitionTablesPattern()).isEqualTo(partitionTables);
    }

    @Test
    void testSerializationAndDeserialization() throws Exception {
        String tables = "public.orders,public.products";
        String partitionTables =
                "public.orders:public.orders_\\d+,public.products:public.products_\\d+";

        PostgresPartitionRules original =
                PostgresPartitionRules.parse(tables, partitionTables, "public");

        // Verify original works
        assertThat(original.hasPartitionRules()).isTrue();
        assertThat(original.getConfiguredParents()).hasSize(2);
        assertThat(original.getChildToParentMappings()).hasSize(2);

        // Serialize
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(original);
        oos.close();

        // Deserialize
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
        PostgresPartitionRules deserialized = (PostgresPartitionRules) ois.readObject();
        ois.close();

        // Verify deserialized object works correctly
        assertThat(deserialized.hasPartitionRules()).isTrue();
        assertThat(deserialized.getConfiguredParents()).hasSize(2);
        assertThat(deserialized.getChildToParentMappings()).hasSize(2);
        assertThat(deserialized.getChildTablePatterns()).hasSize(2);
        assertThat(deserialized.getTablesPattern()).isEqualTo(tables);
        assertThat(deserialized.getPartitionTablesPattern()).isEqualTo(partitionTables);

        // Verify pattern matching still works after deserialization
        java.util.regex.Pattern firstPattern =
                deserialized.getChildToParentMappings().keySet().iterator().next();
        // Pattern format is "^\Qpublic\E\.orders_\d+$" - matches schema.table format
        assertThat(firstPattern.matcher("public.orders_123").matches()).isTrue();
        assertThat(firstPattern.matcher("public.orders_999").matches()).isTrue();
        assertThat(firstPattern.matcher("orders_123").matches()).isFalse();
    }
}
