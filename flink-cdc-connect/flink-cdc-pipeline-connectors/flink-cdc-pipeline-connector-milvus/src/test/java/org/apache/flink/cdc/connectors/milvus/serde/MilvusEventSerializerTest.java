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

package org.apache.flink.cdc.connectors.milvus.serde;

import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests for {@link MilvusEventSerializer}. */
class MilvusEventSerializerTest {

    private static final TableId TABLE_ID = TableId.parse("inventory.docs");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("title", DataTypes.STRING())
                    .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .primaryKey("id")
                    .build();
    private static final Schema PARTITIONED_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("title", DataTypes.STRING())
                    .physicalColumn("category", DataTypes.STRING())
                    .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .primaryKey("id")
                    .build();

    @Test
    void testInsertSerializesToUpsertRow() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("doc-1"),
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f}))));

        Assertions.assertThat(operations).hasSize(1);
        MilvusOperation operation = operations.get(0);
        Assertions.assertThat(operation.getType()).isEqualTo(MilvusOperation.Type.UPSERT);
        Assertions.assertThat(operation.getCollectionName()).isEqualTo("inventory_docs");
        Assertions.assertThat(operation.getRow().get("id").getAsLong()).isEqualTo(1L);
        Assertions.assertThat(operation.getRow().get("title").getAsString()).isEqualTo("doc-1");
        Assertions.assertThat(operation.getRow().get("embedding").getAsJsonArray())
                .hasSize(3)
                .extracting(element -> element.getAsFloat())
                .containsExactly(1.0f, 2.0f, 3.0f);
    }

    @Test
    void testNullScalarSerializesToJsonNull() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                null,
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}))))
                        .get(0);

        Assertions.assertThat(operation.getRow().get("title").isJsonNull()).isTrue();
    }

    @Test
    void testUpdateWithPrimaryKeyChangeDeletesOldKeyBeforeUpsert() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(allowPrimaryKeyChangeConfig());

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("old"),
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})),
                                GenericRecordData.of(
                                        2L,
                                        BinaryStringData.fromString("new"),
                                        new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f}))));

        Assertions.assertThat(operations).hasSize(2);
        Assertions.assertThat(operations.get(0).getType()).isEqualTo(MilvusOperation.Type.DELETE);
        Assertions.assertThat(operations.get(0).getPrimaryKey()).isEqualTo(1L);
        Assertions.assertThat(operations.get(1).getType()).isEqualTo(MilvusOperation.Type.UPSERT);
        Assertions.assertThat(operations.get(1).getRow().get("id").getAsLong()).isEqualTo(2L);
    }

    @Test
    void testUpdateWithPrimaryKeyChangeIsRejectedByDefault() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        Assertions.assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        DataChangeEvent.updateEvent(
                                                TABLE_ID,
                                                GenericRecordData.of(
                                                        1L,
                                                        BinaryStringData.fromString("old"),
                                                        new GenericArrayData(
                                                                new float[] {1.0f, 2.0f, 3.0f})),
                                                GenericRecordData.of(
                                                        2L,
                                                        BinaryStringData.fromString("new"),
                                                        new GenericArrayData(
                                                                new float[] {4.0f, 5.0f, 6.0f})))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rejects UPDATE events that change primary key");
    }

    @Test
    void testUpdateWithSamePrimaryKeyOnlyUpserts() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("old"),
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})),
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("new"),
                                        new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f}))));

        Assertions.assertThat(operations).hasSize(1);
        Assertions.assertThat(operations.get(0).getType()).isEqualTo(MilvusOperation.Type.UPSERT);
        Assertions.assertThat(operations.get(0).getRow().get("title").getAsString())
                .isEqualTo("new");
    }

    @Test
    void testDeleteSerializesToDeleteByPrimaryKey() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.deleteEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("doc-1"),
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f}))));

        Assertions.assertThat(operations).hasSize(1);
        Assertions.assertThat(operations.get(0).getType()).isEqualTo(MilvusOperation.Type.DELETE);
        Assertions.assertThat(operations.get(0).getPrimaryKey()).isEqualTo(1L);
    }

    @Test
    void testDeleteCanBeDisabled() {
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        0,
                        Duration.ZERO,
                        false);
        MilvusEventSerializer serializer = createInitializedSerializer(config);

        Assertions.assertThat(
                        serializer.serialize(
                                DataChangeEvent.deleteEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f})))))
                .isEmpty();
    }

    @Test
    void testConfiguredPartitionFieldIsSerialized() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(partitionConfig(), PARTITIONED_SCHEMA);

        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                BinaryStringData.fromString("manual"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}))))
                        .get(0);

        Assertions.assertThat(operation.getPartitionName()).isEqualTo("manual");
    }

    @Test
    void testPartitionChangeDeletesOldPartitionBeforeUpsert() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(partitionConfig(), PARTITIONED_SCHEMA);

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("old"),
                                        BinaryStringData.fromString("manual"),
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})),
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("new"),
                                        BinaryStringData.fromString("auto"),
                                        new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f}))));

        Assertions.assertThat(operations).hasSize(2);
        Assertions.assertThat(operations.get(0).getType()).isEqualTo(MilvusOperation.Type.DELETE);
        Assertions.assertThat(operations.get(0).getPartitionName()).isEqualTo("manual");
        Assertions.assertThat(operations.get(1).getType()).isEqualTo(MilvusOperation.Type.UPSERT);
        Assertions.assertThat(operations.get(1).getPartitionName()).isEqualTo("auto");
    }

    @Test
    void testPartitionFieldValueIsNormalized() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(partitionConfig(), PARTITIONED_SCHEMA);

        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                BinaryStringData.fromString("manual-docs"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}))))
                        .get(0);

        Assertions.assertThat(operation.getPartitionName()).isEqualTo("manual_docs");
    }

    @Test
    void testNullPartitionFieldWritesDefaultPartition() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(partitionConfig(), PARTITIONED_SCHEMA);

        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                null,
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}))))
                        .get(0);

        Assertions.assertThat(operation.getPartitionName()).isNull();
    }

    @Test
    void testPartitionChangeFromDefaultDeletesDefaultBeforeUpsert() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(partitionConfig(), PARTITIONED_SCHEMA);

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("old"),
                                        null,
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})),
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("new"),
                                        BinaryStringData.fromString("auto"),
                                        new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f}))));

        Assertions.assertThat(operations).hasSize(2);
        Assertions.assertThat(operations.get(0).getType()).isEqualTo(MilvusOperation.Type.DELETE);
        Assertions.assertThat(operations.get(0).getPartitionName()).isNull();
        Assertions.assertThat(operations.get(1).getType()).isEqualTo(MilvusOperation.Type.UPSERT);
        Assertions.assertThat(operations.get(1).getPartitionName()).isEqualTo("auto");
    }

    @Test
    void testPartitionChangeToDefaultDeletesOldPartitionBeforeUpsert() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(partitionConfig(), PARTITIONED_SCHEMA);

        List<MilvusOperation> operations =
                serializer.serialize(
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("old"),
                                        BinaryStringData.fromString("manual"),
                                        new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})),
                                GenericRecordData.of(
                                        1L,
                                        BinaryStringData.fromString("new"),
                                        null,
                                        new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f}))));

        Assertions.assertThat(operations).hasSize(2);
        Assertions.assertThat(operations.get(0).getType()).isEqualTo(MilvusOperation.Type.DELETE);
        Assertions.assertThat(operations.get(0).getPartitionName()).isEqualTo("manual");
        Assertions.assertThat(operations.get(1).getType()).isEqualTo(MilvusOperation.Type.UPSERT);
        Assertions.assertThat(operations.get(1).getPartitionName()).isNull();
    }

    @Test
    void testAddScalarColumnUpdatesWriterSchema() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("category", DataTypes.STRING()))));

        Assertions.assertThat(serializer.serialize(addColumnEvent)).isEmpty();
        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}),
                                                BinaryStringData.fromString("manual"))))
                        .get(0);

        Assertions.assertThat(operation.getRow().get("category").getAsString()).isEqualTo("manual");
    }

    @Test
    void testAddScalarColumnReplayIsIdempotentInWriterSchema() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("category", DataTypes.STRING()))));

        Assertions.assertThat(serializer.serialize(addColumnEvent)).isEmpty();
        Assertions.assertThat(serializer.serialize(addColumnEvent)).isEmpty();

        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}),
                                                BinaryStringData.fromString("manual"))))
                        .get(0);
        Assertions.assertThat(operation.getRow().get("category").getAsString()).isEqualTo("manual");
    }

    @Test
    void testAddScalarColumnReplayWithIncompatibleTypeFails() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());
        serializer.serialize(
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("category", DataTypes.STRING())))));

        Assertions.assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new AddColumnEvent(
                                                TABLE_ID,
                                                Collections.singletonList(
                                                        AddColumnEvent.last(
                                                                Column.physicalColumn(
                                                                        "category",
                                                                        DataTypes.BIGINT()))))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("incompatible with replayed ADD_COLUMN");
    }

    @Test
    void testAlterTableCommentIsNoOpAndKeepsSchema() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        Assertions.assertThat(
                        serializer.serialize(new AlterTableCommentEvent(TABLE_ID, "new comment")))
                .isEmpty();
        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}))))
                        .get(0);

        Assertions.assertThat(operation.getRow().get("title").getAsString()).isEqualTo("doc-1");
    }

    @Test
    void testUnsupportedSchemaChangeDoesNotMutateWriterSchema() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());

        Assertions.assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new DropColumnEvent(
                                                TABLE_ID, Collections.singletonList("title"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("DROP_COLUMN");
        MilvusOperation operation =
                serializer
                        .serialize(
                                DataChangeEvent.insertEvent(
                                        TABLE_ID,
                                        GenericRecordData.of(
                                                1L,
                                                BinaryStringData.fromString("doc-1"),
                                                new GenericArrayData(
                                                        new float[] {1.0f, 2.0f, 3.0f}))))
                        .get(0);

        Assertions.assertThat(operation.getRow().has("title")).isTrue();
    }

    @Test
    void testRejectAddVectorColumnBeforeWriterSchemaMutation() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());
        AddColumnEvent addVectorEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "embedding2",
                                                DataTypes.ARRAY(DataTypes.FLOAT()).notNull()))));
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        Collections.emptyMap(),
                        MilvusTestUtils.defaultConfig().getVectorFields(),
                        Collections.singletonMap(
                                TABLE_ID,
                                Arrays.asList(
                                        MilvusTestUtils.defaultConfig().getVectorFields().get(0),
                                        org.apache.flink.cdc.connectors.milvus.utils
                                                .MilvusVectorFieldSpec.parse(
                                                "embedding2:FloatVector(3)"))),
                        100,
                        Duration.ofSeconds(10),
                        0,
                        Duration.ZERO,
                        true);
        MilvusEventSerializer configuredSerializer = createInitializedSerializer(config);

        Assertions.assertThatThrownBy(() -> configuredSerializer.serialize(addVectorEvent))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support adding vector field embedding2");
    }

    @Test
    void testRejectAddNotNullScalarBeforeWriterSchemaMutation() {
        MilvusEventSerializer serializer =
                createInitializedSerializer(MilvusTestUtils.defaultConfig());
        AddColumnEvent addNotNullEvent =
                new AddColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "category", DataTypes.STRING().notNull()))));

        Assertions.assertThatThrownBy(() -> serializer.serialize(addNotNullEvent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Adding NOT NULL Milvus scalar field");
    }

    @Test
    void testRejectNormalizedCollectionCollisionOnCreateTable() {
        MilvusDataSinkConfig config = MilvusTestUtils.defaultConfig();
        MilvusEventSerializer serializer = new MilvusEventSerializer(config);
        Assertions.assertThat(
                        serializer.serialize(
                                new CreateTableEvent(TableId.parse("db.table-a"), SCHEMA)))
                .isEmpty();

        Assertions.assertThatThrownBy(
                        () ->
                                serializer.serialize(
                                        new CreateTableEvent(TableId.parse("db_table_a"), SCHEMA)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collection name collision");
    }

    private MilvusEventSerializer createInitializedSerializer(MilvusDataSinkConfig config) {
        return createInitializedSerializer(config, SCHEMA);
    }

    private MilvusEventSerializer createInitializedSerializer(
            MilvusDataSinkConfig config, Schema schema) {
        MilvusEventSerializer serializer = new MilvusEventSerializer(config);
        Assertions.assertThat(serializer.serialize(new CreateTableEvent(TABLE_ID, schema)))
                .isEmpty();
        return serializer;
    }

    private MilvusDataSinkConfig allowPrimaryKeyChangeConfig() {
        return new MilvusDataSinkConfig(
                "http://localhost:19530",
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                false,
                "",
                false,
                1024,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                true,
                10,
                true,
                "allow",
                false,
                "",
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }

    private MilvusDataSinkConfig partitionConfig() {
        return new MilvusDataSinkConfig(
                "http://localhost:19530",
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                Collections.emptyMap(),
                true,
                true,
                true,
                false,
                false,
                "category",
                false,
                1024,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                true,
                10,
                true,
                "reject",
                false,
                "",
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }
}
