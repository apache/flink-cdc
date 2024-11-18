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

package org.apache.flink.cdc.runtime.serializer.schema;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.util.Collections;

/** A test for the {@link SchemaSerializer}. */
class SchemaSerializerTest extends SerializerTestBase<Schema> {
    @Override
    protected TypeSerializer<Schema> createSerializer() {
        return SchemaSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<Schema> getTypeClass() {
        return Schema.class;
    }

    @Override
    protected Schema[] getTestData() {
        return new Schema[] {
            Schema.newBuilder()
                    .comment("test")
                    .physicalColumn("col1", DataTypes.BIGINT())
                    .physicalColumn("col2", DataTypes.TIME(), "comment")
                    .metadataColumn("m1", DataTypes.BOOLEAN())
                    .metadataColumn("m2", DataTypes.DATE(), "mKey")
                    .metadataColumn("m3", DataTypes.TIMESTAMP_LTZ(), "mKey", "comment")
                    .option("option", "fake")
                    .primaryKey(Collections.singletonList("col1"))
                    .partitionKey(Collections.singletonList("m2"))
                    .build(),
            Schema.newBuilder()
                    .physicalColumn("col1", DataTypes.BIGINT())
                    .physicalColumn("col2", DataTypes.TIME(), "comment")
                    .metadataColumn("m1", DataTypes.BOOLEAN())
                    .metadataColumn("m2", DataTypes.DATE(), "mKey")
                    .metadataColumn("m3", DataTypes.TIMESTAMP_LTZ(), "mKey", "comment")
                    .build()
        };
    }
}
