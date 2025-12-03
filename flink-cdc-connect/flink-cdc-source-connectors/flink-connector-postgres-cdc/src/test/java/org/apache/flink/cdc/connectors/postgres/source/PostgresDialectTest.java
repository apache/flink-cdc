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

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for {@link PostgresDialect}. */
class PostgresDialectTest extends PostgresTestBase {

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres1",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres2",
                    "inventory",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private final UniqueDatabase inventoryPartitionedDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres3",
                    "inventory_partitioned",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    void testDiscoverDataCollectionsInMultiDatabases() {

        // initial two databases in same postgres instance
        customDatabase.createAndInitialize();
        inventoryDatabase.createAndInitialize();

        // get table named 'customer.customers' from customDatabase which is actual in
        // inventoryDatabase
        PostgresSourceConfigFactory configFactoryOfCustomDatabase =
                getMockPostgresSourceConfigFactory(customDatabase, "customer", "Customers", 10);
        PostgresDialect dialectOfcustomDatabase =
                new PostgresDialect(configFactoryOfCustomDatabase.create(0));
        List<TableId> tableIdsOfcustomDatabase =
                dialectOfcustomDatabase.discoverDataCollections(
                        configFactoryOfCustomDatabase.create(0));
        Assertions.assertThat(tableIdsOfcustomDatabase.get(0)).hasToString("customer.Customers");

        // get table named 'inventory.products' from customDatabase which is actual in
        // inventoryDatabase
        // however, nothing is found
        PostgresSourceConfigFactory configFactoryOfInventoryDatabase =
                getMockPostgresSourceConfigFactory(inventoryDatabase, "inventory", "products", 10);
        PostgresDialect dialectOfInventoryDatabase =
                new PostgresDialect(configFactoryOfInventoryDatabase.create(0));
        List<TableId> tableIdsOfInventoryDatabase =
                dialectOfInventoryDatabase.discoverDataCollections(
                        configFactoryOfInventoryDatabase.create(0));
        Assertions.assertThat(tableIdsOfInventoryDatabase.get(0)).hasToString("inventory.products");

        // get table named 'customer.customers' from customDatabase which is actual not in
        // customDatabase
        // however, something is found
        PostgresSourceConfigFactory configFactoryOfInventoryDatabase2 =
                getMockPostgresSourceConfigFactory(inventoryDatabase, "customer", "customers", 10);
        PostgresDialect dialectOfInventoryDatabase2 =
                new PostgresDialect(configFactoryOfInventoryDatabase2.create(0));
        List<TableId> tableIdsOfInventoryDatabase2 =
                dialectOfInventoryDatabase2.discoverDataCollections(
                        configFactoryOfInventoryDatabase2.create(0));
        Assertions.assertThat(tableIdsOfInventoryDatabase2).isEmpty();
    }

    @Test
    void testDiscoverDataCollectionsForPartitionedTable() {
        // initial database with partitioned table
        inventoryPartitionedDatabase.createAndInitialize();

        // get table named 'inventory_partitioned.products' from inventoryPartitionedDatabase
        PostgresSourceConfigFactory configFactoryOfInventoryPartitionedDatabase =
                getMockPostgresSourceConfigFactory(
                        inventoryPartitionedDatabase, "inventory_partitioned", "products", 10);
        configFactoryOfInventoryPartitionedDatabase.setIncludePartitionedTables(true);
        PostgresDialect dialectOfInventoryPartitionedDatabase =
                new PostgresDialect(configFactoryOfInventoryPartitionedDatabase.create(0));
        List<TableId> tableIdsOfInventoryPartitionedDatabase =
                dialectOfInventoryPartitionedDatabase.discoverDataCollections(
                        configFactoryOfInventoryPartitionedDatabase.create(0));
        Assertions.assertThat(tableIdsOfInventoryPartitionedDatabase.get(0))
                .hasToString("inventory_partitioned.products");
    }
}
