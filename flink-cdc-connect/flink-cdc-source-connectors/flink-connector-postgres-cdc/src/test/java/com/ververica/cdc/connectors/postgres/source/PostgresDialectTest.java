/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.postgres.source;

import com.ververica.cdc.connectors.postgres.PostgresTestBase;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.testutils.UniqueDatabase;
import io.debezium.relational.TableId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Tests for {@link PostgresDialect}. */
public class PostgresDialectTest extends PostgresTestBase {

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

    @Test
    public void testDiscoverDataCollectionsInMultiDatabases() {

        // initial two databases in same postgres instance
        customDatabase.createAndInitialize();
        inventoryDatabase.createAndInitialize();

        // get table named 'customer.customers' from customDatabase which is actual in
        // inventoryDatabase
        PostgresSourceConfigFactory configFactoryOfCustomDatabase =
                getMockPostgresSourceConfigFactory(customDatabase, "customer", "customers", 10);
        PostgresDialect dialectOfcustomDatabase =
                new PostgresDialect(configFactoryOfCustomDatabase.create(0));
        List<TableId> tableIdsOfcustomDatabase =
                dialectOfcustomDatabase.discoverDataCollections(
                        configFactoryOfCustomDatabase.create(0));
        Assert.assertEquals(tableIdsOfcustomDatabase.get(0).toString(), "customer.customers");

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
        Assert.assertEquals(tableIdsOfInventoryDatabase.get(0).toString(), "inventory.products");

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
        Assert.assertTrue(tableIdsOfInventoryDatabase2.isEmpty());
    }
}
