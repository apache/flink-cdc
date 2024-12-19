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

package org.apache.flink.cdc.migration.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaDerivation;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/** Dummy classes for migration test. Called via reflection. */
public class SchemaRegistryMigrationMock implements MigrationMockBase {
    private static final TableId TABLE_1 = TableId.tableId("ns", "scm", "tbl1");
    private static final TableId TABLE_2 = TableId.tableId("ns", "scm", "tbl2");

    private static Schema genSchema(String identifier) {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.INT())
                .physicalColumn("col_" + identifier, DataTypes.STRING())
                .primaryKey("id")
                .build();
    }

    private static final Map<TableId, SortedMap<Integer, Schema>> ORIGINAL_SCHEMA_MAP;
    private static final Map<TableId, SortedMap<Integer, Schema>> EVOLVED_SCHEMA_MAP;

    static {
        SortedMap<Integer, Schema> originalSchemas = new TreeMap<>();
        originalSchemas.put(1, genSchema("upstream_1"));
        originalSchemas.put(2, genSchema("upstream_2"));
        originalSchemas.put(3, genSchema("upstream_3"));

        SortedMap<Integer, Schema> evolvedSchemas = new TreeMap<>();
        evolvedSchemas.put(1, genSchema("evolved_1"));
        evolvedSchemas.put(2, genSchema("evolved_2"));
        evolvedSchemas.put(3, genSchema("evolved_3"));

        ORIGINAL_SCHEMA_MAP = new HashMap<>();
        ORIGINAL_SCHEMA_MAP.put(TABLE_1, originalSchemas);
        ORIGINAL_SCHEMA_MAP.put(TABLE_2, originalSchemas);

        EVOLVED_SCHEMA_MAP = new HashMap<>();
        EVOLVED_SCHEMA_MAP.put(TABLE_1, evolvedSchemas);
        EVOLVED_SCHEMA_MAP.put(TABLE_2, evolvedSchemas);
    }

    public SchemaManager generateDummySchemaManager() {
        return new SchemaManager(new HashMap<>(), new HashMap<>(), SchemaChangeBehavior.EVOLVE);
    }

    public SchemaRegistry generateSchemaRegistry() {
        return new SchemaRegistry(
                "Dummy Name",
                null,
                Executors.newSingleThreadExecutor(),
                e -> {},
                new ArrayList<>());
    }

    private SchemaManager getSchemaManager(SchemaRegistry schemaRegistry) throws Exception {
        Field managerField = SchemaRegistry.class.getDeclaredField("schemaManager");
        managerField.setAccessible(true);
        return (SchemaManager) managerField.get(schemaRegistry);
    }

    @SuppressWarnings("unchecked")
    private Map<TableId, SortedMap<Integer, Schema>> getOriginalSchemaMap(
            SchemaRegistry schemaRegistry) throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field originalField = SchemaManager.class.getDeclaredField("originalSchemas");
        originalField.setAccessible(true);
        return (Map<TableId, SortedMap<Integer, Schema>>) originalField.get(schemaManager);
    }

    private void setOriginalSchemaMap(
            SchemaRegistry schemaRegistry,
            Map<TableId, SortedMap<Integer, Schema>> originalSchemaMap)
            throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field field = SchemaManager.class.getDeclaredField("originalSchemas");
        field.setAccessible(true);
        field.set(schemaManager, originalSchemaMap);
    }

    @SuppressWarnings("unchecked")
    private Map<TableId, SortedMap<Integer, Schema>> getEvolvedSchemaMap(
            SchemaRegistry schemaRegistry) throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field originalField = SchemaManager.class.getDeclaredField("evolvedSchemas");
        originalField.setAccessible(true);
        return (Map<TableId, SortedMap<Integer, Schema>>) originalField.get(schemaManager);
    }

    private void setEvolvedSchemaMap(
            SchemaRegistry schemaRegistry,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemaMap)
            throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field field = SchemaManager.class.getDeclaredField("evolvedSchemas");
        field.setAccessible(true);
        field.set(schemaManager, evolvedSchemaMap);
    }

    private SchemaDerivation getSchemaDerivation(SchemaRegistry schemaRegistry) throws Exception {
        Field field = SchemaRegistry.class.getDeclaredField("schemaDerivation");
        field.setAccessible(true);
        return (SchemaDerivation) field.get(schemaRegistry);
    }

    @SuppressWarnings("unchecked")
    private List<Tuple2<Selectors, TableId>> getSchemaRoutes(SchemaRegistry schemaRegistry)
            throws Exception {
        SchemaDerivation schemaDerivation = getSchemaDerivation(schemaRegistry);
        Field field = SchemaDerivation.class.getDeclaredField("routes");
        field.setAccessible(true);
        return (List<Tuple2<Selectors, TableId>>) field.get(schemaDerivation);
    }

    @Override
    public int getSerializerVersion() {
        return -1;
    }

    @Override
    public byte[] serializeObject() throws Exception {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        SchemaRegistry registry = generateSchemaRegistry();
        setOriginalSchemaMap(registry, ORIGINAL_SCHEMA_MAP);
        setEvolvedSchemaMap(registry, EVOLVED_SCHEMA_MAP);

        registry.checkpointCoordinator(0, future);

        while (!future.isDone()) {
            Thread.sleep(1000);
        }
        return future.get();
    }

    @Override
    public boolean deserializeAndCheckObject(int v, byte[] b) throws Exception {
        SchemaRegistry expected = generateSchemaRegistry();
        setOriginalSchemaMap(expected, ORIGINAL_SCHEMA_MAP);
        setEvolvedSchemaMap(expected, EVOLVED_SCHEMA_MAP);

        SchemaRegistry actual = generateSchemaRegistry();
        actual.resetToCheckpoint(0, b);

        return getOriginalSchemaMap(expected).equals(getOriginalSchemaMap(actual))
                && getEvolvedSchemaMap(expected).equals(getEvolvedSchemaMap(actual))
                && getSchemaRoutes(expected).equals(getSchemaRoutes(actual));
    }
}
