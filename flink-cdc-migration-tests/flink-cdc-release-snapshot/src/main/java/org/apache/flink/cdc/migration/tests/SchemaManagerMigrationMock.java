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
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.schema.coordinator.SchemaManager;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/** Dummy classes for migration test. Called via reflection. */
public class SchemaManagerMigrationMock implements MigrationMockBase {
    private static final TableId DUMMY_TABLE_ID =
            TableId.tableId("dummyNamespace", "dummySchema", "dummyTable");
    private static final Schema DUMMY_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("age", DataTypes.DOUBLE())
                    .primaryKey("id", "name")
                    .build();

    public SchemaManager generateDummyObject() {
        SortedMap<Integer, Schema> schemaVersions = new TreeMap<>();
        schemaVersions.put(1, DUMMY_SCHEMA);
        schemaVersions.put(2, DUMMY_SCHEMA);
        schemaVersions.put(3, DUMMY_SCHEMA);
        return new SchemaManager(Collections.singletonMap(DUMMY_TABLE_ID, schemaVersions));
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
