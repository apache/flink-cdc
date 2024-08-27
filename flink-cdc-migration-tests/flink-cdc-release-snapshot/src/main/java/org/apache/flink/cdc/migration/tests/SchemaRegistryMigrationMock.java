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
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaDerivation;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaRegistry;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
        return new SchemaManager(
                Collections.singletonMap(DUMMY_TABLE_ID, schemaVersions),
                Collections.singletonMap(DUMMY_TABLE_ID, schemaVersions),
                SchemaChangeBehavior.EVOLVE);
    }

    public SchemaRegistry generateSchemaRegistry() {
        return new SchemaRegistry(
                "Dummy Name",
                null,
                Executors.newFixedThreadPool(1),
                new MetadataApplier() {
                    @Override
                    public boolean acceptsSchemaEvolutionType(
                            SchemaChangeEventType schemaChangeEventType) {
                        return true;
                    }

                    @Override
                    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
                        return Arrays.stream(SchemaChangeEventTypeFamily.ALL)
                                .collect(Collectors.toSet());
                    }

                    @Override
                    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
                        // Do nothing
                    }
                },
                new ArrayList<>(),
                SchemaChangeBehavior.EVOLVE);
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

    private SchemaDerivation getSchemaDerivation(SchemaRegistry schemaRegistry) throws Exception {
        Field field = SchemaRegistry.class.getDeclaredField("schemaDerivation");
        field.setAccessible(true);
        return (SchemaDerivation) field.get(schemaRegistry);
    }

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
        setSchemaManager(expected, generateDummySchemaManager());
        SchemaRegistry actual = generateSchemaRegistry();
        actual.resetToCheckpoint(0, b);
        return getSchemaManager(expected).equals(getSchemaManager(actual))
                && getSchemaRoutes(expected).equals(getSchemaRoutes(actual));
    }
}
