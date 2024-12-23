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
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/** Dummy classes for migration test. Called via reflection. */
public class SchemaManagerMigrationMock implements MigrationMockBase {
    private static final TableId TABLE_1 = TableId.tableId("ns", "scm", "tbl1");
    private static final TableId TABLE_2 = TableId.tableId("ns", "scm", "tbl2");

    private static final String SCHEMA_MANAGER =
            "runtime.operators.schema.coordinator.SchemaManager";

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

    public SchemaManager generateDummyObject() {
        return new SchemaManager(
                ORIGINAL_SCHEMA_MAP, EVOLVED_SCHEMA_MAP, SchemaChangeBehavior.TRY_EVOLVE);
    }

    @Override
    public int getSerializerVersion() {
        return SchemaManager.SERIALIZER.getVersion();
    }

    @Override
    public byte[] serializeObject() throws Exception {
        return SchemaManager.SERIALIZER.serialize(generateDummyObject());
    }

    @Override
    public boolean deserializeAndCheckObject(int version, byte[] serialized) throws Exception {
        Object expected = generateDummyObject();
        Object actual = SchemaManager.SERIALIZER.deserialize(version, serialized);
        return expected.equals(actual);
    }
}
