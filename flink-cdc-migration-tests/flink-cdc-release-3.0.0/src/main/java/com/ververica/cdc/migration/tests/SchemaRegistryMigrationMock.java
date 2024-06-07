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

package com.ververica.cdc.migration.tests;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaManager;
import com.ververica.cdc.runtime.operators.schema.coordinator.SchemaRegistry;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/** Dummy classes for migration test. Called via reflection. */
public class SchemaRegistryMigrationMock implements MigrationMockBase {
    private static final TableId DUMMY_TABLE_ID =
            TableId.tableId("dummyNamespace", "dummySchema", "dummyTable");
    private static final Schema DUMMY_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("age", DataTypes.DOUBLE())
                    .primaryKey("id", "name")
                    .build();

    public SchemaManager generateDummySchemaManager() {
        SortedMap<Integer, Schema> schemaVersions = new TreeMap<>();
        schemaVersions.put(1, DUMMY_SCHEMA);
        schemaVersions.put(2, DUMMY_SCHEMA);
        schemaVersions.put(3, DUMMY_SCHEMA);
        return new SchemaManager(Collections.singletonMap(DUMMY_TABLE_ID, schemaVersions));
    }

    public SchemaRegistry generateSchemaRegistry() {
        return new SchemaRegistry("Dummy Name", null, e -> {});
    }

    private SchemaManager getSchemaManager(SchemaRegistry schemaRegistry) throws Exception {
        Field field = SchemaRegistry.class.getDeclaredField("schemaManager");
        field.setAccessible(true);
        return (SchemaManager) field.get(schemaRegistry);
    }

    private void setSchemaManager(SchemaRegistry schemaRegistry, SchemaManager schemaManager)
            throws Exception {
        Field field = SchemaRegistry.class.getDeclaredField("schemaManager");
        field.setAccessible(true);
        field.set(schemaRegistry, schemaManager);
    }

    @Override
    public int getSerializerVersion() {
        return -1;
    }

    @Override
    public byte[] serializeObject() throws Exception {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        SchemaRegistry registry = generateSchemaRegistry();

        setSchemaManager(registry, generateDummySchemaManager());
        registry.checkpointCoordinator(0, future);

        while (!future.isDone()) {
            Thread.sleep(1000);
        }
        return future.get();
    }

    @Override
    public boolean deserializeAndCheckObject(int v, byte[] b) throws Exception {
        SchemaRegistry expected = generateSchemaRegistry();
        SchemaRegistry actual = generateSchemaRegistry();
        actual.resetToCheckpoint(0, b);
        return getSchemaManager(expected).equals(getSchemaManager(actual));
    }
}
