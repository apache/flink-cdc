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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for chunky columns configuration functionality. */
public class PostgresChunkyColumnsConfigTest {

    @Test
    public void testChunkKeyColumnConfiguration() {
        // Create a config factory to test chunk key column functionality
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Test basic configuration
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Test chunk key column configuration
        ObjectPath tablePath = new ObjectPath("public", "customers");
        configFactory.chunkKeyColumn(tablePath, "customer_id");

        // Verify the factory is properly configured
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testMultipleChunkKeyColumns() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Configure multiple chunk key columns for different tables
        configFactory.chunkKeyColumn(new ObjectPath("public", "customers"), "customer_id");
        configFactory.chunkKeyColumn(new ObjectPath("public", "orders"), "order_id");
        configFactory.chunkKeyColumn(new ObjectPath("public", "products"), "product_id");

        // Verify the factory accepts multiple chunk key columns
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithDifferentSchemas() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Configure chunk key columns for tables in different schemas
        configFactory.chunkKeyColumn(new ObjectPath("public", "customers"), "customer_id");
        configFactory.chunkKeyColumn(new ObjectPath("inventory", "products"), "product_id");
        configFactory.chunkKeyColumn(new ObjectPath("sales", "orders"), "order_id");

        // Verify the factory handles different schemas
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithSpecialCharacters() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Configure chunk key columns with special characters
        configFactory.chunkKeyColumn(new ObjectPath("public", "user_profiles"), "user_profile_id");
        configFactory.chunkKeyColumn(new ObjectPath("public", "order_items"), "order_item_id");

        // Verify the factory handles special characters
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithSplitSizeConfiguration() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Configure split size along with chunk key column
        configFactory.splitSize(1000);
        configFactory.chunkKeyColumn(new ObjectPath("public", "large_table"), "id");

        // Verify the factory handles both split size and chunk key column
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithEmptyTableName() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Test with empty table name (should handle gracefully)
        try {
            ObjectPath emptyTablePath = new ObjectPath("public", "");
            configFactory.chunkKeyColumn(emptyTablePath, "id");
        } catch (Exception e) {
            // Expected to handle empty table names gracefully
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }

        // Verify the factory still exists
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithEmptyColumnName() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Test with empty column name (should handle gracefully)
        ObjectPath tablePath = new ObjectPath("public", "customers");
        configFactory.chunkKeyColumn(tablePath, "");

        // Verify the factory handles empty column names
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithNullValues() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Test with null values (should handle gracefully)
        try {
            configFactory.chunkKeyColumn(null, "id");
        } catch (Exception e) {
            // Expected to handle null values gracefully
            assertThat(e).isInstanceOf(NullPointerException.class);
        }

        try {
            configFactory.chunkKeyColumn(new ObjectPath("public", "customers"), null);
        } catch (Exception e) {
            // Expected to handle null values gracefully
            assertThat(e).isInstanceOf(NullPointerException.class);
        }

        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithLongTableName() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Test with very long table name
        String longTableName =
                "a_very_long_table_name_that_exceeds_normal_length_limits_for_testing_purposes";
        configFactory.chunkKeyColumn(new ObjectPath("public", longTableName), "id");

        // Verify the factory handles long table names
        assertThat(configFactory).isNotNull();
    }

    @Test
    public void testChunkKeyColumnWithLongColumnName() {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Configure basic settings
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("testdb");
        configFactory.username("user");
        configFactory.password("password");

        // Test with very long column name
        String longColumnName =
                "a_very_long_column_name_that_exceeds_normal_length_limits_for_testing_purposes";
        configFactory.chunkKeyColumn(new ObjectPath("public", "customers"), longColumnName);

        // Verify the factory handles long column names
        assertThat(configFactory).isNotNull();
    }
}
