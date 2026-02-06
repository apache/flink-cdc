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

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresPartitionConnectorConfig}. */
class PostgresPartitionConnectorConfigTest {

    /** Creates a PostgresPartitionConnectorConfig with a valid router from config. */
    private PostgresPartitionConnectorConfig createConfigWithRouter(Configuration config) {
        String partitionTables = config.getString(PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY);
        PostgresPartitionRules rules = PostgresPartitionRules.parse(null, partitionTables, null);
        PostgresPartitionRouter router = new PostgresPartitionRouter(true, rules);
        return new PostgresPartitionConnectorConfig(config, router);
    }

    @Test
    void testChildPatternWithAnchorIsProperlyHandled() {
        // Config with anchored pattern: ^orders_\d+$
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "public.orders:^orders_\\d+$")
                        .with("table.include.list", "public.orders")
                        .build();

        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Child table should be excluded (matched by anchored pattern)
        TableId childTable = new TableId(null, "public", "orders_202401");
        assertThat(filter.isIncluded(childTable)).isFalse();

        // Parent table should be included
        TableId parentTable = new TableId(null, "public", "orders");
        assertThat(filter.isIncluded(parentTable)).isTrue();

        // Non-matching table should not be excluded by child pattern
        TableId otherTable = new TableId(null, "public", "users");
        // This depends on base filter, but should not match child pattern
        assertThat(filter.isIncluded(otherTable)).isFalse();
    }

    @Test
    void testChildPatternWithLeadingAnchorOnly() {
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "public.orders:^orders_\\d{6}")
                        .with("table.include.list", "public.orders")
                        .build();

        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Child table should be excluded
        TableId childTable = new TableId(null, "public", "orders_202401");
        assertThat(filter.isIncluded(childTable)).isFalse();
    }

    @Test
    void testChildPatternWithTrailingAnchorOnly() {
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "public:orders_\\d+$")
                        .with("table.include.list", "public.orders")
                        .build();

        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Child table should be excluded
        TableId childTable = new TableId(null, "public", "orders_202401");
        assertThat(filter.isIncluded(childTable)).isFalse();
    }

    @Test
    void testChildOnlyPatternGeneratesExclusionFilter() {
        // Use colon format: parent:child_regex
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "public.orders:public.orders_\\d{6}")
                        .with("table.include.list", "public.orders,public.orders_.*")
                        .build();

        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Child table matching pattern should be excluded
        TableId childTable = new TableId(null, "public", "orders_202401");
        assertThat(filter.isIncluded(childTable)).isFalse();

        // Parent table should be included
        TableId parentTable = new TableId(null, "public", "orders");
        assertThat(filter.isIncluded(parentTable)).isTrue();
    }

    @Test
    void testEscapedDotInSchemaIsHandled() {
        // Use colon format with schema
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "my_schema.orders:my_schema.orders_\\d{6}")
                        .with("table.include.list", "my_schema.orders,my_schema.orders_.*")
                        .build();

        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Child table should be excluded
        TableId childTable = new TableId(null, "my_schema", "orders_202401");
        assertThat(filter.isIncluded(childTable)).isFalse();

        // Parent table should be included
        TableId parentTable = new TableId(null, "my_schema", "orders");
        assertThat(filter.isIncluded(parentTable)).isTrue();
    }

    @Test
    void testMultiplePartitionTablesWithMixedFormats() {
        // Multiple partition tables with colon format
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "public.orders:public.orders_\\d{6},"
                                        + "public.vouchers:public.vouchers_\\d{6},"
                                        + "sales.invoices:sales.invoices_\\d+")
                        .with(
                                "table.include.list",
                                "public.orders,public.vouchers,sales.invoices,"
                                        + "public.orders_.*,public.vouchers_.*,sales.invoices_.*")
                        .build();

        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // All child tables should be excluded
        assertThat(filter.isIncluded(new TableId(null, "public", "orders_202401"))).isFalse();
        assertThat(filter.isIncluded(new TableId(null, "public", "vouchers_202401"))).isFalse();
        assertThat(filter.isIncluded(new TableId(null, "sales", "invoices_12345"))).isFalse();

        // All parent tables should be included
        assertThat(filter.isIncluded(new TableId(null, "public", "orders"))).isTrue();
        assertThat(filter.isIncluded(new TableId(null, "public", "vouchers"))).isTrue();
        assertThat(filter.isIncluded(new TableId(null, "sales", "invoices"))).isTrue();
    }

    @Test
    void testEscapedDollarSignIsNotTreatedAsAnchor() {
        // Pattern with escaped dollar sign (literal $) should not be stripped
        // e.g., orders_\$ matches table names ending with literal $
        Configuration config =
                TestHelper.defaultConfig()
                        .with(PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY, "public:orders_\\$")
                        .with("table.include.list", "public.orders")
                        .build();

        // Should not throw PatternSyntaxException
        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Table with literal $ should be excluded
        TableId childTable = new TableId(null, "public", "orders_$");
        assertThat(filter.isIncluded(childTable)).isFalse();

        // Parent table should be included
        TableId parentTable = new TableId(null, "public", "orders");
        assertThat(filter.isIncluded(parentTable)).isTrue();
    }

    @Test
    void testEscapedCaretIsNotTreatedAsAnchor() {
        // Pattern with escaped caret (literal ^) should not be stripped
        Configuration config =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceConfig.PARTITION_TABLES_CONFIG_KEY,
                                "public:\\^orders_\\d+")
                        .with("table.include.list", "public.orders")
                        .build();

        // Should not throw PatternSyntaxException
        PostgresPartitionConnectorConfig connectorConfig = createConfigWithRouter(config);
        Tables.TableFilter filter = connectorConfig.getTableFilters().dataCollectionFilter();

        // Table starting with literal ^ should be excluded
        TableId childTable = new TableId(null, "public", "^orders_123");
        assertThat(filter.isIncluded(childTable)).isFalse();
    }
}
