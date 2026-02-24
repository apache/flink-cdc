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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PostTransformChangeInfo} and its inner {@link
 * PostTransformChangeInfo.Serializer}.
 */
class PostTransformChangeInfoTest {

    private static final PostTransformChangeInfo.Serializer SERIALIZER =
            PostTransformChangeInfo.SERIALIZER;

    @Test
    void testSerializerVersionNumber() {
        assertThat(SERIALIZER.getVersion()).isEqualTo(2);
    }

    @Test
    void testSerializerRoundtrip() throws Exception {
        TableId tableId = TableId.tableId("my_namespace", "my_schema", "my_table");
        Schema preSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema postSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("computed", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        PostTransformChangeInfo original =
                PostTransformChangeInfo.of(tableId, preSchema, postSchema);

        byte[] serialized = SERIALIZER.serialize(original);
        PostTransformChangeInfo deserialized =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized);

        assertThat(deserialized.getTableId()).isEqualTo(tableId);
        assertThat(deserialized.getPreTransformedSchema()).isEqualTo(preSchema);
        assertThat(deserialized.getPostTransformedSchema()).isEqualTo(postSchema);
    }

    @Test
    void testSerializerRoundtripWithComplexSchemas() throws Exception {
        TableId tableId = TableId.tableId("db", "public", "complex_table");
        Schema preSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("active", DataTypes.BOOLEAN())
                        .physicalColumn("score", DataTypes.DOUBLE())
                        .physicalColumn("created_at", DataTypes.TIMESTAMP(3))
                        .physicalColumn("amount", DataTypes.DECIMAL(10, 2))
                        .physicalColumn("data", DataTypes.BYTES())
                        .primaryKey("id")
                        .build();
        Schema postSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .physicalColumn("active", DataTypes.BOOLEAN())
                        .physicalColumn("score", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        PostTransformChangeInfo original =
                PostTransformChangeInfo.of(tableId, preSchema, postSchema);

        byte[] serialized = SERIALIZER.serialize(original);
        PostTransformChangeInfo deserialized =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized);

        assertThat(deserialized.getTableId()).isEqualTo(tableId);
        assertThat(deserialized.getPreTransformedSchema()).isEqualTo(preSchema);
        assertThat(deserialized.getPostTransformedSchema()).isEqualTo(postSchema);
        assertThat(deserialized.getPreTransformedSchema().getColumns()).hasSize(7);
        assertThat(deserialized.getPostTransformedSchema().getColumns()).hasSize(4);
    }

    @Test
    void testSerializerRoundtripWithSimpleTableId() throws Exception {
        // Test with a TableId that has only a table name (no namespace/schema)
        TableId tableId = TableId.tableId("simple_table");
        Schema preSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT())
                        .physicalColumn("value", DataTypes.FLOAT())
                        .build();
        Schema postSchema = Schema.newBuilder().physicalColumn("id", DataTypes.BIGINT()).build();

        PostTransformChangeInfo original =
                PostTransformChangeInfo.of(tableId, preSchema, postSchema);

        byte[] serialized = SERIALIZER.serialize(original);
        PostTransformChangeInfo deserialized =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized);

        assertThat(deserialized.getTableId()).isEqualTo(tableId);
        assertThat(deserialized.getPreTransformedSchema()).isEqualTo(preSchema);
        assertThat(deserialized.getPostTransformedSchema()).isEqualTo(postSchema);
    }

    @Test
    void testBackwardCompatibleDeserialization() throws Exception {
        // Manually construct bytes in the OLD format (without magic marker):
        // Old format: tableId + preSchema + postSchema (no magic marker, no version int)
        TableId tableId = TableId.tableId("ns", "sch", "tbl");
        Schema preSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema postSchema =
                Schema.newBuilder().physicalColumn("id", DataTypes.INT()).primaryKey("id").build();

        // Serialize in old format: just tableId + preSchema + postSchema
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out =
                new DataOutputViewStreamWrapper(new DataOutputStream(baos));
        TableIdSerializer.INSTANCE.serialize(tableId, out);
        SchemaSerializer.INSTANCE.serialize(preSchema, out);
        SchemaSerializer.INSTANCE.serialize(postSchema, out);
        byte[] oldFormatBytes = baos.toByteArray();

        // Deserialize using the new Serializer — it should detect old format
        PostTransformChangeInfo deserialized =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), oldFormatBytes);

        assertThat(deserialized.getTableId()).isEqualTo(tableId);
        assertThat(deserialized.getPreTransformedSchema()).isEqualTo(preSchema);
        assertThat(deserialized.getPostTransformedSchema()).isEqualTo(postSchema);
    }

    @Test
    void testBackwardCompatibleDeserializationWithComplexSchema() throws Exception {
        // Old format with more complex schemas
        TableId tableId = TableId.tableId("mydb", "myschema", "users");
        Schema preSchema =
                Schema.newBuilder()
                        .physicalColumn("user_id", DataTypes.BIGINT())
                        .physicalColumn("username", DataTypes.STRING())
                        .physicalColumn("email", DataTypes.VARCHAR(255))
                        .physicalColumn("age", DataTypes.INT())
                        .primaryKey("user_id")
                        .build();
        Schema postSchema =
                Schema.newBuilder()
                        .physicalColumn("user_id", DataTypes.BIGINT())
                        .physicalColumn("username", DataTypes.STRING())
                        .physicalColumn("email", DataTypes.VARCHAR(255))
                        .primaryKey("user_id")
                        .build();

        // Serialize in old format
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out =
                new DataOutputViewStreamWrapper(new DataOutputStream(baos));
        TableIdSerializer.INSTANCE.serialize(tableId, out);
        SchemaSerializer.INSTANCE.serialize(preSchema, out);
        SchemaSerializer.INSTANCE.serialize(postSchema, out);
        byte[] oldFormatBytes = baos.toByteArray();

        PostTransformChangeInfo deserialized =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), oldFormatBytes);

        assertThat(deserialized.getTableId()).isEqualTo(tableId);
        assertThat(deserialized.getPreTransformedSchema()).isEqualTo(preSchema);
        assertThat(deserialized.getPostTransformedSchema()).isEqualTo(postSchema);
    }

    @Test
    void testNewFormatAndOldFormatProduceSameResult() throws Exception {
        // Verify that serializing with new format and deserializing gives the same result
        // as deserializing from old format
        TableId tableId = TableId.tableId("db", "schema", "table");
        Schema preSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("data", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        Schema postSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT())
                        .physicalColumn("data", DataTypes.STRING())
                        .physicalColumn("extra", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        // New format: serialize via SERIALIZER
        PostTransformChangeInfo original =
                PostTransformChangeInfo.of(tableId, preSchema, postSchema);
        byte[] newFormatBytes = SERIALIZER.serialize(original);
        PostTransformChangeInfo fromNewFormat =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), newFormatBytes);

        // Old format: manually serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out =
                new DataOutputViewStreamWrapper(new DataOutputStream(baos));
        TableIdSerializer.INSTANCE.serialize(tableId, out);
        SchemaSerializer.INSTANCE.serialize(preSchema, out);
        SchemaSerializer.INSTANCE.serialize(postSchema, out);
        byte[] oldFormatBytes = baos.toByteArray();
        PostTransformChangeInfo fromOldFormat =
                SERIALIZER.deserialize(SERIALIZER.getVersion(), oldFormatBytes);

        // Both should produce equivalent results
        assertThat(fromNewFormat.getTableId()).isEqualTo(fromOldFormat.getTableId());
        assertThat(fromNewFormat.getPreTransformedSchema())
                .isEqualTo(fromOldFormat.getPreTransformedSchema());
        assertThat(fromNewFormat.getPostTransformedSchema())
                .isEqualTo(fromOldFormat.getPostTransformedSchema());
    }
}
