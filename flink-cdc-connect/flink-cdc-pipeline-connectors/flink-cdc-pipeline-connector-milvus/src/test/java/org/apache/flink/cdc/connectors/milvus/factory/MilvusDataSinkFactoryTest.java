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

package org.apache.flink.cdc.connectors.milvus.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSink;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

/** Tests for {@link MilvusDataSinkFactory}. */
class MilvusDataSinkFactoryTest {

    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(MilvusDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("uri", "http://localhost:19530")
                                .put("token", "root:Milvus")
                                .put("database-name", "default")
                                .put("vector.fields", "embedding:FloatVector(3)")
                                .put(
                                        "vector.fields.inventory.docs",
                                        "image_embedding:FloatVector(4)")
                                .put("collection.mapping", "inventory.docs:docs_collection")
                                .put("sink.flush.max-rows", "10")
                                .put("sink.flush.interval", "5s")
                                .put("sink.max-retries", "2")
                                .put("sink.retry.backoff", "100ms")
                                .put("sink.adaptive-batch-split.enabled", "true")
                                .put("sink.adaptive-batch-split.min-rows", "20")
                                .put("load-collection.enabled", "true")
                                .put("partition.field", "category")
                                .put("partition.auto-create.enabled", "true")
                                .put("partition.auto-create.max-count", "64")
                                .put("sink.primary-key-change.mode", "allow")
                                .put("consistency-level", "strong")
                                .put("index.type", "HNSW")
                                .put("index.metric-type", "L2")
                                .put("index.params.M", "16")
                                .build());

        DataSink dataSink = createDataSink(sinkFactory, conf);

        Assertions.assertThat(dataSink).isInstanceOf(MilvusDataSink.class);
        MilvusDataSinkConfig config = extractConfig(dataSink);
        Assertions.assertThat(config.getUri()).isEqualTo("http://localhost:19530");
        Assertions.assertThat(config.getToken()).isEqualTo("root:Milvus");
        Assertions.assertThat(config.getCollectionMappings())
                .containsEntry(TableId.parse("inventory.docs"), "docs_collection");
        Assertions.assertThat(config.getVectorFields()).hasSize(1);
        Assertions.assertThat(config.getVectorFields(TableId.parse("inventory.docs")))
                .hasSize(1)
                .first()
                .extracting("fieldName", "dimension")
                .containsExactly("image_embedding", 4);
        Assertions.assertThat(config.getFlushMaxRows()).isEqualTo(10);
        Assertions.assertThat(config.getIndexParams()).containsEntry("M", 16);
        Assertions.assertThat(config.isLoadCollectionEnabled()).isTrue();
        Assertions.assertThat(config.getPartitionField()).isEqualTo("category");
        Assertions.assertThat(config.isPartitionAutoCreateEnabled()).isTrue();
        Assertions.assertThat(config.getPartitionAutoCreateMaxCount()).isEqualTo(64);
        Assertions.assertThat(config.getPrimaryKeyChangeMode()).isEqualTo("allow");
        Assertions.assertThat(config.getPartitionNames()).isEmpty();
        Assertions.assertThat(config.getAdaptiveBatchSplitMinRows()).isEqualTo(20);
    }

    @Test
    void testMissingRequiredUri() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(ImmutableMap.of("vector.fields", "embedding:FloatVector(3)"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("uri");
    }

    @Test
    void testMissingRequiredVectorFields() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(ImmutableMap.of("uri", "http://localhost:19530"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("vector.fields");
    }

    @Test
    void testRejectInvalidVectorFieldDefinition() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri", "http://localhost:19530",
                                "vector.fields", "embedding:BinaryVector(128)"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid Milvus vector field definition");
    }

    @Test
    void testRejectDuplicateVectorFieldDefinition() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3),embedding:FloatVector(4)"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Duplicate Milvus vector field embedding");
    }

    @Test
    void testRejectNonPositiveFlushRows() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "sink.flush.max-rows",
                                "0"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.flush.max-rows");
    }

    @Test
    void testRejectInvalidIndexType() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "index.type",
                                "bad_index"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("index.type");
    }

    @Test
    void testRejectInvalidConsistencyLevel() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "consistency-level",
                                "bad_level"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("consistency-level");
    }

    @Test
    void testRejectMalformedCollectionMapping() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "collection.mapping",
                                "inventory.docs"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("collection.mapping");
    }

    @Test
    void testRejectDuplicateCollectionMapping() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "collection.mapping",
                                "inventory.docs:docs_a;inventory.docs:docs_b"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("duplicate mapping for table inventory.docs");
    }

    @Test
    void testRejectEmptyIndexParamKey() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("uri", "http://localhost:19530")
                                .put("vector.fields", "embedding:FloatVector(3)")
                                .put("index.params.", "16")
                                .build());

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("index.params. keys must not be empty");
    }

    @Test
    void testRejectAllowNoPrimaryKey() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "sink.allow-no-primary-key",
                                "true"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.allow-no-primary-key is not supported");
    }

    @Test
    void testRejectInvalidPartitionFieldName() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "partition.field",
                                "bad-name"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Milvus partition field name");
    }

    @Test
    void testRejectNonPositiveAdaptiveSplitMinRows() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "sink.adaptive-batch-split.min-rows",
                                "0"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.adaptive-batch-split.min-rows");
    }

    @Test
    void testRejectNonPositivePartitionAutoCreateMaxCount() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "partition.auto-create.max-count",
                                "0"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("partition.auto-create.max-count");
    }

    @Test
    void testRejectInvalidPrimaryKeyChangeMode() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "sink.primary-key-change.mode",
                                "delete-and-insert"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.primary-key-change.mode");
    }

    @Test
    void testParsePartitionNames() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "partition.names",
                                "manual,auto"));

        MilvusDataSinkConfig config = extractConfig(createDataSink(sinkFactory, conf));

        Assertions.assertThat(config.getPartitionNames()).containsExactly("manual", "auto");
    }

    @Test
    void testRejectReservedPartitionName() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "partition.names",
                                "_default"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("partition.names must not include");
    }

    @Test
    void testPartitionNamesCanPrecreatePartitionsForPartitionFieldRouting() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "partition.names",
                                "manual",
                                "partition.field",
                                "category"));

        MilvusDataSinkConfig config = extractConfig(createDataSink(sinkFactory, conf));

        Assertions.assertThat(config.getPartitionField()).isEqualTo("category");
        Assertions.assertThat(config.getPartitionNames()).containsExactly("manual");
    }

    @Test
    void testRejectNormalizedCollectionCollisionForTableVectorFields() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("uri", "http://localhost:19530")
                                .put("vector.fields", "embedding:FloatVector(3)")
                                .put("vector.fields.db.table-a", "embedding:FloatVector(3)")
                                .put("vector.fields.db_table_a", "embedding:FloatVector(3)")
                                .build());

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("collection name collision");
    }

    @Test
    void testRejectNormalizedCollectionCollisionForCollisionCheckTables() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("milvus", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of(
                                "uri",
                                "http://localhost:19530",
                                "vector.fields",
                                "embedding:FloatVector(3)",
                                "collection.collision-check.tables",
                                "db.table-a,db_table_a"));

        Assertions.assertThatThrownBy(() -> createDataSink(sinkFactory, conf))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("collection name collision");
    }

    private DataSink createDataSink(DataSinkFactory sinkFactory, Configuration conf) {
        return sinkFactory.createDataSink(
                new FactoryHelper.DefaultContext(
                        conf, conf, Thread.currentThread().getContextClassLoader()));
    }

    private MilvusDataSinkConfig extractConfig(DataSink dataSink) {
        try {
            Field field = MilvusDataSink.class.getDeclaredField("config");
            field.setAccessible(true);
            return (MilvusDataSinkConfig) field.get(dataSink);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
