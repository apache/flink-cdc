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

package org.apache.flink.cdc.connectors.milvus.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusTestUtils;

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link MilvusCollectionUtils}. */
class MilvusCollectionUtilsTest {

    private static final TableId TABLE_ID = TableId.parse("inventory.docs");

    @Test
    void testBuildCreateCollectionReq() {
        MilvusDataSinkConfig config =
                MilvusTestUtils.config(
                        "http://localhost:19530",
                        Collections.emptyMap(),
                        Collections.singletonList(
                                MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                        Collections.emptyMap(),
                        100,
                        java.time.Duration.ofSeconds(10),
                        0,
                        java.time.Duration.ZERO,
                        true,
                        true,
                        "strong");

        CreateCollectionReq req =
                MilvusCollectionUtils.buildCreateCollectionReq(TABLE_ID, validSchema(), config);

        Assertions.assertThat(req.getDatabaseName()).isEqualTo("default");
        Assertions.assertThat(req.getCollectionName()).isEqualTo("inventory_docs");
        Assertions.assertThat(req.getConsistencyLevel()).isEqualTo(ConsistencyLevel.STRONG);
        Assertions.assertThat(req.getIndexParams()).hasSize(1);
        Assertions.assertThat(req.getCollectionSchema().getField("id").getDataType())
                .isEqualTo(DataType.Int64);
        Assertions.assertThat(req.getCollectionSchema().getField("id").getIsPrimaryKey()).isTrue();
        Assertions.assertThat(req.getCollectionSchema().getField("id").getAutoID()).isFalse();
        Assertions.assertThat(req.getCollectionSchema().getField("title").getDataType())
                .isEqualTo(DataType.VarChar);
        Assertions.assertThat(req.getCollectionSchema().getField("embedding").getDataType())
                .isEqualTo(DataType.FloatVector);
        Assertions.assertThat(req.getCollectionSchema().getField("embedding").getDimension())
                .isEqualTo(3);
    }

    @Test
    void testRejectConfiguredVectorFieldAbsentFromSchema() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, schema, MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is absent from table");
    }

    @Test
    void testRejectInvalidPhysicalFieldName() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("bad-name", DataTypes.STRING())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, schema, MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Milvus field name");
    }

    @Test
    void testRejectNullableVectorField() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, schema, MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be NOT NULL");
    }

    @Test
    void testAcceptDoubleArrayVectorSourceType() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.DOUBLE()).notNull())
                        .primaryKey("id")
                        .build();

        CreateCollectionReq req =
                MilvusCollectionUtils.buildCreateCollectionReq(
                        TABLE_ID, schema, MilvusTestUtils.defaultConfig());

        Assertions.assertThat(req.getCollectionSchema().getField("embedding").getDataType())
                .isEqualTo(DataType.FloatVector);
    }

    @Test
    void testRejectUnsupportedVectorSourceType() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.INT()).notNull())
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, schema, MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ARRAY<FLOAT>, ARRAY<DOUBLE>, or a JSON array string");
    }

    @Test
    void testJsonStringVectorSourceTypeIsAccepted() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("embedding", DataTypes.STRING().notNull())
                        .primaryKey("id")
                        .build();

        CreateCollectionReq req =
                MilvusCollectionUtils.buildCreateCollectionReq(
                        TABLE_ID, schema, MilvusTestUtils.defaultConfig());

        Assertions.assertThat(req.getCollectionSchema().getField("embedding").getDataType())
                .isEqualTo(DataType.FloatVector);
    }

    @Test
    void testRejectVectorFieldAsPrimaryKey() {
        MilvusDataSinkConfig config =
                new MilvusDataSinkConfig(
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
                        Collections.singletonList(MilvusVectorFieldSpec.parse("id:FloatVector(3)")),
                        Collections.emptyMap(),
                        "id",
                        65535,
                        100,
                        java.time.Duration.ofSeconds(10),
                        0,
                        java.time.Duration.ZERO,
                        true,
                        10,
                        true,
                        "reject",
                        false,
                        "",
                        "AUTOINDEX",
                        "COSINE",
                        Collections.emptyMap());
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .primaryKey("id")
                        .build();

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, schema, config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot also be the primary key");
    }

    @Test
    void testRejectMissingPartitionField() {
        MilvusDataSinkConfig config = partitionConfig("category");

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, validSchema(), config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Configured Milvus partition field category is absent");
    }

    @Test
    void testRejectVectorPartitionField() {
        MilvusDataSinkConfig config = partitionConfig("embedding");

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildCreateCollectionReq(
                                        TABLE_ID, validSchema(), config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot also be a vector field");
    }

    @Test
    void testValidateExistingCollectionRejectsPrimaryKeyTypeMismatch() {
        DescribeCollectionResp existing =
                existingCollection(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.VarChar)
                                .isPrimaryKey(true)
                                .autoID(false)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(3)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.validateExistingCollection(
                                        TABLE_ID,
                                        validSchema(),
                                        existing,
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary key field id type mismatch");
    }

    @Test
    void testValidateExistingCollectionRejectsAutoIdPrimaryKey() {
        DescribeCollectionResp existing =
                existingCollection(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .autoID(true)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(3)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.validateExistingCollection(
                                        TABLE_ID,
                                        validSchema(),
                                        existing,
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("autoID");
    }

    @Test
    void testValidateExistingCollectionRejectsVectorDimensionMismatch() {
        DescribeCollectionResp existing =
                existingCollection(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .autoID(false)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(4)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.validateExistingCollection(
                                        TABLE_ID,
                                        validSchema(),
                                        existing,
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    void testValidateExistingCollectionRejectsMissingScalarField() {
        DescribeCollectionResp existing =
                existingCollection(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .autoID(false)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(3)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.validateExistingCollection(
                                        TABLE_ID,
                                        validSchema(),
                                        existing,
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("misses scalar field title");
    }

    @Test
    void testValidateExistingCollectionRejectsScalarTypeMismatch() {
        DescribeCollectionResp existing =
                existingCollection(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .autoID(false)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("title")
                                .dataType(DataType.Int64)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(3)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.validateExistingCollection(
                                        TABLE_ID,
                                        validSchema(),
                                        existing,
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scalar field title type mismatch");
    }

    @Test
    void testValidateExistingCollectionRejectsShortVarCharScalar() {
        DescribeCollectionResp existing =
                existingCollection(
                        CreateCollectionReq.FieldSchema.builder()
                                .name("id")
                                .dataType(DataType.Int64)
                                .isPrimaryKey(true)
                                .autoID(false)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("title")
                                .dataType(DataType.VarChar)
                                .maxLength(8)
                                .build(),
                        CreateCollectionReq.FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.FloatVector)
                                .dimension(3)
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.validateExistingCollection(
                                        TABLE_ID,
                                        validSchema(),
                                        existing,
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("max length is too small");
    }

    @Test
    void testBuildAddScalarFieldRejectsNotNullColumn() {
        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.buildAddScalarFieldReq(
                                        Column.physicalColumn(
                                                "category", DataTypes.STRING().notNull()),
                                        MilvusTestUtils.defaultConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Adding NOT NULL Milvus scalar field");
    }

    @Test
    void testRejectNormalizedCollectionCollision() {
        Map<String, TableId> owners = new HashMap<>();
        MilvusDataSinkConfig config = MilvusTestUtils.defaultConfig();

        MilvusCollectionUtils.registerCollectionName(TableId.parse("db.table-a"), config, owners);

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.registerCollectionName(
                                        TableId.parse("db_table_a"), config, owners))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collection name collision");
    }

    private static Schema validSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("title", DataTypes.STRING())
                .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                .primaryKey("id")
                .build();
    }

    private static MilvusDataSinkConfig partitionConfig(String partitionField) {
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
                partitionField,
                false,
                1024,
                Collections.emptyList(),
                MilvusTestUtils.defaultConfig().getVectorFields(),
                Collections.emptyMap(),
                "",
                65535,
                100,
                java.time.Duration.ofSeconds(10),
                0,
                java.time.Duration.ZERO,
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

    private static DescribeCollectionResp existingCollection(
            CreateCollectionReq.FieldSchema... fields) {
        CreateCollectionReq.CollectionSchema collectionSchema =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(java.util.Arrays.asList(fields))
                        .build();
        return DescribeCollectionResp.builder()
                .collectionName("inventory_docs")
                .databaseName("default")
                .primaryFieldName("id")
                .autoID(false)
                .collectionSchema(collectionSchema)
                .build();
    }
}
