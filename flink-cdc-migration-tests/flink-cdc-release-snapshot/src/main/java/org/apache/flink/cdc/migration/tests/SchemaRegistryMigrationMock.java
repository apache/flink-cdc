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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaManager;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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

    public SchemaCoordinator generateSchemaRegistry() {
        SchemaCoordinator coordinator =
                new SchemaCoordinator(
                        "Dummy Name",
                        MOCKED_CONTEXT,
                        Executors.newSingleThreadExecutor(),
                        e -> {},
                        new ArrayList<>(),
                        SchemaChangeBehavior.EVOLVE,
                        Duration.ofMinutes(3));
        try {
            coordinator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return coordinator;
    }

    private SchemaManager getSchemaManager(SchemaCoordinator schemaCoordinator) throws Exception {
        Field managerField =
                SchemaCoordinator.class.getSuperclass().getDeclaredField("schemaManager");
        managerField.setAccessible(true);
        return (SchemaManager) managerField.get(schemaCoordinator);
    }

    @SuppressWarnings("unchecked")
    private Map<TableId, SortedMap<Integer, Schema>> getOriginalSchemaMap(
            SchemaCoordinator schemaRegistry) throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field originalField = SchemaManager.class.getDeclaredField("originalSchemas");
        originalField.setAccessible(true);
        return (Map<TableId, SortedMap<Integer, Schema>>) originalField.get(schemaManager);
    }

    private void setOriginalSchemaMap(
            SchemaCoordinator schemaRegistry,
            Map<TableId, SortedMap<Integer, Schema>> originalSchemaMap)
            throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field field = SchemaManager.class.getDeclaredField("originalSchemas");
        field.setAccessible(true);
        field.set(schemaManager, originalSchemaMap);
    }

    @SuppressWarnings("unchecked")
    private Map<TableId, SortedMap<Integer, Schema>> getEvolvedSchemaMap(
            SchemaCoordinator schemaRegistry) throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field originalField = SchemaManager.class.getDeclaredField("evolvedSchemas");
        originalField.setAccessible(true);
        return (Map<TableId, SortedMap<Integer, Schema>>) originalField.get(schemaManager);
    }

    private void setEvolvedSchemaMap(
            SchemaCoordinator schemaRegistry,
            Map<TableId, SortedMap<Integer, Schema>> evolvedSchemaMap)
            throws Exception {
        SchemaManager schemaManager = getSchemaManager(schemaRegistry);
        Field field = SchemaManager.class.getDeclaredField("evolvedSchemas");
        field.setAccessible(true);
        field.set(schemaManager, evolvedSchemaMap);
    }

    @Override
    public int getSerializerVersion() {
        return -1;
    }

    @Override
    public byte[] serializeObject() throws Exception {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        SchemaCoordinator registry = generateSchemaRegistry();
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
        SchemaCoordinator expected = generateSchemaRegistry();
        setOriginalSchemaMap(expected, ORIGINAL_SCHEMA_MAP);
        setEvolvedSchemaMap(expected, EVOLVED_SCHEMA_MAP);

        SchemaCoordinator actual = generateSchemaRegistry();
        actual.resetToCheckpoint(0, b);

        return getOriginalSchemaMap(expected).equals(getOriginalSchemaMap(actual))
                && getEvolvedSchemaMap(expected).equals(getEvolvedSchemaMap(actual));
    }

    private static final OperatorCoordinator.Context MOCKED_CONTEXT =
            new OperatorCoordinator.Context() {

                @Override
                public OperatorID getOperatorId() {
                    return null;
                }

                @Override
                public OperatorCoordinatorMetricGroup metricGroup() {
                    return null;
                }

                @Override
                public void failJob(Throwable throwable) {}

                @Override
                public int currentParallelism() {
                    return 0;
                }

                @Override
                public ClassLoader getUserCodeClassloader() {
                    return null;
                }

                @Override
                public CoordinatorStore getCoordinatorStore() {
                    return null;
                }

                @Override
                public boolean isConcurrentExecutionAttemptsSupported() {
                    return false;
                }

                @Nullable
                @Override
                public CheckpointCoordinator getCheckpointCoordinator() {
                    return null;
                }
            };
}
