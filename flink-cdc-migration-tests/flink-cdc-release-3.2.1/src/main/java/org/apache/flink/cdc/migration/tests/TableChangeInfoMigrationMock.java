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
import org.apache.flink.cdc.runtime.operators.transform.PreTransformChangeInfo;

/** Dummy classes for migration test. Called via reflection. */
public class TableChangeInfoMigrationMock implements MigrationMockBase {
    private static final TableId DUMMY_TABLE_ID =
            TableId.tableId("dummyNamespace", "dummySchema", "dummyTable");
    private static final Schema DUMMY_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("age", DataTypes.DOUBLE())
                    .primaryKey("id", "name")
                    .build();

    public PreTransformChangeInfo generateDummyObject() {
        return PreTransformChangeInfo.of(DUMMY_TABLE_ID, DUMMY_SCHEMA, DUMMY_SCHEMA);
    }

    @Override
    public int getSerializerVersion() {
        return PreTransformChangeInfo.SERIALIZER.getVersion();
    }

    @Override
    public byte[] serializeObject() throws Exception {
        return PreTransformChangeInfo.SERIALIZER.serialize(generateDummyObject());
    }

    @Override
    public boolean deserializeAndCheckObject(int version, byte[] bytes) throws Exception {
        PreTransformChangeInfo expected = generateDummyObject();
        PreTransformChangeInfo actual =
                PreTransformChangeInfo.SERIALIZER.deserialize(version, bytes);

        return expected.getTableId().equals(actual.getTableId())
                && expected.getSourceSchema().equals(actual.getSourceSchema())
                && expected.getPreTransformedSchema().equals(actual.getPreTransformedSchema());
    }
}
