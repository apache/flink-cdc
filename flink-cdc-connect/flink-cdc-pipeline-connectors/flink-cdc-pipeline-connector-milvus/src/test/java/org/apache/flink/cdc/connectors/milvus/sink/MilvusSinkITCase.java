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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusClientUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Integration tests for Milvus sink against a Dockerized standalone Milvus. */
class MilvusSinkITCase {

    private static final TableId DOCS_TABLE = TableId.parse("inventory.docs");
    private static final TableId TAGS_TABLE = TableId.parse("inventory.tags");
    private static final String DOCS_COLLECTION = "inventory_docs";
    private static final String TAGS_COLLECTION = "inventory_tags";
    private static final int MILVUS_PORT = 19530;
    private static final int MILVUS_HEALTH_PORT = 9091;

    private static GenericContainer<?> milvusContainer;
    private static String milvusUri;
    private static String milvusToken;
    private static final AtomicBoolean RESTART_IT_FAILURE_TRIGGERED = new AtomicBoolean(false);
    private static final AtomicReference<Throwable> RESTART_IT_CHECKPOINT_ASSERTION_FAILURE =
            new AtomicReference<>();

    private MilvusDataSinkConfig sinkConfig;

    @BeforeAll
    static void startMilvus() {
        milvusUri =
                firstNonBlank(System.getProperty("milvus.it.uri"), System.getenv("MILVUS_IT_URI"));
        milvusToken =
                firstNonBlank(
                        System.getProperty("milvus.it.token"), System.getenv("MILVUS_IT_TOKEN"));
        if (milvusUri != null) {
            return;
        }

        try {
            Assumptions.assumeTrue(
                    DockerClientFactory.instance().isDockerAvailable(), "Docker is unavailable.");
        } catch (RuntimeException e) {
            Assumptions.abort("Docker is unavailable: " + e.getMessage());
        }

        milvusContainer =
                new GenericContainer<>(DockerImageName.parse("milvusdb/milvus:v2.6.0"))
                        .withExposedPorts(MILVUS_PORT, MILVUS_HEALTH_PORT)
                        .withEnv("ETCD_USE_EMBED", "true")
                        .withEnv("DEPLOY_MODE", "STANDALONE")
                        .withEnv("COMMON_STORAGETYPE", "local")
                        .withEnv("QUOTAANDLIMITS_FLUSHRATE_ENABLED", "false")
                        .withCommand("milvus", "run", "standalone")
                        .waitingFor(
                                Wait.forHttp("/healthz")
                                        .forPort(MILVUS_HEALTH_PORT)
                                        .forStatusCode(200)
                                        .withStartupTimeout(Duration.ofMinutes(5)));
        milvusContainer.start();
        milvusUri =
                "http://"
                        + milvusContainer.getHost()
                        + ":"
                        + milvusContainer.getMappedPort(MILVUS_PORT);
    }

    @AfterAll
    static void stopMilvus() {
        if (milvusContainer != null) {
            milvusContainer.stop();
        }
    }

    @BeforeEach
    void setUp() {
        sinkConfig =
                config(
                        milvusUri,
                        Collections.emptyMap(),
                        Collections.singletonList(
                                MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(100),
                        true,
                        true,
                        "strong");
        Awaitility.await("Milvus health check")
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofSeconds(1))
                .failFast(
                        "Milvus container exited before becoming healthy.",
                        () -> milvusContainer != null && !milvusContainer.isRunning())
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            MilvusClientV2 client = MilvusClientUtils.createClient(sinkConfig);
                            try {
                                Assertions.assertThat(client.checkHealth().getIsHealthy()).isTrue();
                            } finally {
                                client.close();
                            }
                        });
        dropCollectionIfExists(DOCS_COLLECTION);
        dropCollectionIfExists(TAGS_COLLECTION);
    }

    @Test
    void testVectorUpsertUpdatePrimaryKeyChangeAndDelete() throws Exception {
        Schema schema = docsSchema();
        MilvusDataSinkConfig allowPrimaryKeyChangeConfig = allowPrimaryKeyChangeConfig();
        new MilvusMetadataApplier(allowPrimaryKeyChangeConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (MilvusEventWriter writer = new MilvusEventWriter(allowPrimaryKeyChangeConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(insert(DOCS_TABLE, 1L, "doc-1", 1.0f), new MockContext());
            writer.flush(false);
            Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 1L).get("title")).isEqualTo("doc-1");

            writer.write(
                    DataChangeEvent.updateEvent(
                            DOCS_TABLE,
                            record(1L, "doc-1", 1.0f),
                            record(1L, "doc-1-updated", 4.0f)),
                    new MockContext());
            writer.flush(false);
            Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 1L).get("title"))
                    .isEqualTo("doc-1-updated");

            writer.write(
                    DataChangeEvent.updateEvent(
                            DOCS_TABLE,
                            record(1L, "doc-1-updated", 4.0f),
                            record(2L, "doc-2", 7.0f)),
                    new MockContext());
            writer.flush(false);
            awaitEntityAbsent(DOCS_COLLECTION, 1L);
            Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 2L).get("title")).isEqualTo("doc-2");

            writer.write(
                    DataChangeEvent.deleteEvent(DOCS_TABLE, record(2L, "doc-2", 7.0f)),
                    new MockContext());
            writer.flush(false);
        }

        awaitEntityAbsent(DOCS_COLLECTION, 2L);
    }

    @Test
    void testAddScalarColumnAndWrite() throws Exception {
        Schema schema = docsSchema();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        DOCS_TABLE,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("category", DataTypes.STRING()))));

        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));
        new MilvusMetadataApplier(sinkConfig).applySchemaChange(addColumnEvent);
        DescribeCollectionResp collection = describeCollection(DOCS_COLLECTION);
        Assertions.assertThat(collection.getCollectionSchema().getField("category").getDataType())
                .isEqualTo(DataType.VarChar);

        Schema extendedSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("title", DataTypes.STRING())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                        .physicalColumn("category", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        try (MilvusEventWriter writer = new MilvusEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(addColumnEvent, new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            GenericRecordData.of(
                                    3L,
                                    BinaryStringData.fromString("doc-3"),
                                    new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f}),
                                    BinaryStringData.fromString("manual"))),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 3L).get("category")).isEqualTo("manual");
        Assertions.assertThat(extendedSchema.getColumn("category")).isPresent();
    }

    @Test
    void testCheckpointReplayConvergesToSameEntity() throws Exception {
        Schema schema = docsSchema();
        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (MilvusEventWriter writer = new MilvusEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(insert(DOCS_TABLE, 10L, "doc-10", 1.0f), new MockContext());
            writer.flush(true);
            writer.write(insert(DOCS_TABLE, 10L, "doc-10", 1.0f), new MockContext());
            writer.flush(true);
        }

        Map<String, Object> entity = awaitEntity(DOCS_COLLECTION, 10L);
        Assertions.assertThat(entity.get("title")).isEqualTo("doc-10");
    }

    @Test
    void testFlinkCheckpointRestartWithSchemaReplayConverges() throws Exception {
        Schema schema = docsSchema();
        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        RESTART_IT_FAILURE_TRIGGERED.set(false);
        RESTART_IT_CHECKPOINT_ASSERTION_FAILURE.set(null);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);

        env.addSource(
                        new CheckpointRestartSource(DOCS_TABLE, schema),
                        TypeInformation.of(Event.class))
                .returns(new EventTypeInfo())
                .setParallelism(1)
                .sinkTo(new MilvusEventSink(sinkConfig))
                .setParallelism(1);

        env.execute("Milvus checkpoint restart sink test");

        Throwable checkpointAssertionFailure = RESTART_IT_CHECKPOINT_ASSERTION_FAILURE.get();
        if (checkpointAssertionFailure != null) {
            throw new AssertionError(
                    "Milvus insert was not visible after a completed Flink checkpoint.",
                    checkpointAssertionFailure);
        }
        Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 20L).get("title"))
                .isEqualTo("doc-20-after-restart");
    }

    @Test
    void testExistingCollectionValidationIsIdempotent() throws Exception {
        Schema schema = docsSchema();
        MilvusMetadataApplier metadataApplier = new MilvusMetadataApplier(sinkConfig);

        metadataApplier.applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        Assertions.assertThatCode(
                        () ->
                                metadataApplier.applySchemaChange(
                                        new CreateTableEvent(DOCS_TABLE, schema)))
                .doesNotThrowAnyException();
    }

    @Test
    void testPrimaryKeyChangeIsRejectedByDefaultBeforeMilvusWrite() throws Exception {
        Schema schema = docsSchema();
        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (MilvusEventWriter writer = new MilvusEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(insert(DOCS_TABLE, 11L, "doc-11", 1.0f), new MockContext());
            writer.flush(false);

            Assertions.assertThatThrownBy(
                            () ->
                                    writer.write(
                                            DataChangeEvent.updateEvent(
                                                    DOCS_TABLE,
                                                    record(11L, "doc-11", 1.0f),
                                                    record(12L, "doc-12", 4.0f)),
                                            new MockContext()))
                    .isInstanceOf(IOException.class)
                    .hasRootCauseMessage(
                            "Milvus sink rejects UPDATE events that change primary key by default. Set sink.primary-key-change.mode=allow only when collection-level routing is acceptable.");
        }

        Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 11L).get("title")).isEqualTo("doc-11");
        awaitEntityAbsent(DOCS_COLLECTION, 12L);
    }

    @Test
    void testVectorDimensionMismatchFailsBeforeMilvusWrite() throws Exception {
        Schema schema = docsSchema();
        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (MilvusEventWriter writer = new MilvusEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            Assertions.assertThatThrownBy(
                            () ->
                                    writer.write(
                                            DataChangeEvent.insertEvent(
                                                    DOCS_TABLE,
                                                    GenericRecordData.of(
                                                            4L,
                                                            BinaryStringData.fromString("bad"),
                                                            new GenericArrayData(
                                                                    new float[] {1.0f, 2.0f}))),
                                            new MockContext()))
                    .isInstanceOf(IOException.class)
                    .hasRootCauseMessage(
                            "Vector field embedding dimension mismatch. Expected 3 but was 2.");
        }

        awaitEntityAbsent(DOCS_COLLECTION, 4L);
    }

    @Test
    void testStringPrimaryKeyAndJsonStringVector() throws Exception {
        MilvusDataSinkConfig tagConfig =
                config(
                        milvusUri,
                        Collections.emptyMap(),
                        Collections.singletonList(
                                MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(100),
                        true,
                        true,
                        "strong");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.STRING().notNull())
                        .physicalColumn("embedding", DataTypes.STRING().notNull())
                        .primaryKey("id")
                        .build();

        new MilvusMetadataApplier(tagConfig)
                .applySchemaChange(new CreateTableEvent(TAGS_TABLE, schema));

        try (MilvusEventWriter writer = new MilvusEventWriter(tagConfig)) {
            writer.write(new CreateTableEvent(TAGS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            TAGS_TABLE,
                            GenericRecordData.of(
                                    BinaryStringData.fromString("tag-1"),
                                    BinaryStringData.fromString("[1.0,2.0,3.0]"))),
                    new MockContext());
            writer.flush(false);
        }

        Map<String, Object> entity = awaitEntity(TAGS_COLLECTION, "tag-1");
        Assertions.assertThat(entity.get("id")).isEqualTo("tag-1");
    }

    @Test
    void testPartitionPreCreateAndPartitionFieldWrite() throws Exception {
        Schema schema = partitionedDocsSchema();
        MilvusDataSinkConfig partitionConfig =
                config(
                        milvusUri,
                        Collections.emptyMap(),
                        Collections.singletonList(
                                MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(100),
                        true,
                        true,
                        "strong",
                        false,
                        "category",
                        false,
                        Collections.singletonList("manual"));

        new MilvusMetadataApplier(partitionConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));
        Assertions.assertThat(hasPartition(DOCS_COLLECTION, "manual")).isTrue();

        try (MilvusEventWriter writer = new MilvusEventWriter(partitionConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    partitionedInsert(DOCS_TABLE, 5L, "doc-5", "manual", 1.0f), new MockContext());
            writer.flush(false);
        }

        Map<String, Object> entity = awaitEntity(DOCS_COLLECTION, 5L);
        Assertions.assertThat(entity.get("title")).isEqualTo("doc-5");
        Assertions.assertThat(entity.get("category")).isEqualTo("manual");
    }

    @Test
    void testLoadCollectionOptionLoadsAfterCreateTable() throws Exception {
        Schema schema = docsSchema();
        MilvusDataSinkConfig loadConfig =
                config(
                        milvusUri,
                        Collections.emptyMap(),
                        Collections.singletonList(
                                MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(100),
                        true,
                        true,
                        "strong",
                        true,
                        "",
                        false,
                        Collections.emptyList());

        new MilvusMetadataApplier(loadConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        Assertions.assertThat(isCollectionLoaded(DOCS_COLLECTION)).isTrue();
    }

    @Test
    void testDoubleArrayVectorSourceWritesToMilvus() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("title", DataTypes.STRING())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.DOUBLE()).notNull())
                        .primaryKey("id")
                        .build();

        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (MilvusEventWriter writer = new MilvusEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            GenericRecordData.of(
                                    6L,
                                    BinaryStringData.fromString("doc-double"),
                                    new GenericArrayData(new double[] {1.0d, 2.0d, 3.0d}))),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 6L).get("title"))
                .isEqualTo("doc-double");
    }

    @Test
    void testRuntimePartitionAutoCreate() throws Exception {
        Schema schema = partitionedDocsSchema();
        MilvusDataSinkConfig partitionConfig =
                config(
                        milvusUri,
                        Collections.emptyMap(),
                        Collections.singletonList(
                                MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                        Collections.emptyMap(),
                        100,
                        Duration.ofSeconds(10),
                        1,
                        Duration.ofMillis(100),
                        true,
                        true,
                        "strong",
                        false,
                        "category",
                        true,
                        Collections.emptyList());

        new MilvusMetadataApplier(partitionConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));
        Assertions.assertThat(hasPartition(DOCS_COLLECTION, "runtime")).isFalse();

        try (MilvusEventWriter writer = new MilvusEventWriter(partitionConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    partitionedInsert(DOCS_TABLE, 7L, "doc-7", "runtime", 1.0f), new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(hasPartition(DOCS_COLLECTION, "runtime")).isTrue();
        Map<String, Object> entity = awaitEntity(DOCS_COLLECTION, 7L);
        Assertions.assertThat(entity.get("title")).isEqualTo("doc-7");
        Assertions.assertThat(entity.get("category")).isEqualTo("runtime");
    }

    @Test
    void testAddScalarColumnReplayIsIdempotentAgainstMilvus() throws Exception {
        Schema schema = docsSchema();
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        DOCS_TABLE,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("tenant", DataTypes.STRING()))));

        MilvusMetadataApplier metadataApplier = new MilvusMetadataApplier(sinkConfig);
        metadataApplier.applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));
        metadataApplier.applySchemaChange(addColumnEvent);
        metadataApplier.applySchemaChange(addColumnEvent);

        DescribeCollectionResp collection = describeCollection(DOCS_COLLECTION);
        Assertions.assertThat(collection.getCollectionSchema().getField("tenant").getDataType())
                .isEqualTo(DataType.VarChar);
    }

    @Test
    void testAlterTableCommentIsNoOpAndDataStillWrites() throws Exception {
        Schema schema = docsSchema();
        AlterTableCommentEvent alterTableCommentEvent =
                new AlterTableCommentEvent(DOCS_TABLE, "docs collection comment");

        new MilvusMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));
        new MilvusMetadataApplier(sinkConfig).applySchemaChange(alterTableCommentEvent);

        try (MilvusEventWriter writer = new MilvusEventWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(alterTableCommentEvent, new MockContext());
            writer.write(insert(DOCS_TABLE, 8L, "doc-comment", 1.0f), new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 8L).get("title"))
                .isEqualTo("doc-comment");
    }

    private static Schema docsSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("title", DataTypes.STRING())
                .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                .primaryKey("id")
                .build();
    }

    private static Schema partitionedDocsSchema() {
        return Schema.newBuilder()
                .physicalColumn("id", DataTypes.BIGINT().notNull())
                .physicalColumn("title", DataTypes.STRING())
                .physicalColumn("category", DataTypes.STRING())
                .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                .primaryKey("id")
                .build();
    }

    private static DataChangeEvent insert(
            TableId tableId, long id, String title, float firstVectorValue) {
        return DataChangeEvent.insertEvent(tableId, record(id, title, firstVectorValue));
    }

    private static GenericRecordData record(long id, String title, float firstVectorValue) {
        return GenericRecordData.of(
                id,
                BinaryStringData.fromString(title),
                new GenericArrayData(
                        new float[] {
                            firstVectorValue, firstVectorValue + 1, firstVectorValue + 2
                        }));
    }

    private static DataChangeEvent partitionedInsert(
            TableId tableId, long id, String title, String category, float firstVectorValue) {
        return DataChangeEvent.insertEvent(
                tableId, partitionedRecord(id, title, category, firstVectorValue));
    }

    private static GenericRecordData partitionedRecord(
            long id, String title, String category, float firstVectorValue) {
        return GenericRecordData.of(
                id,
                BinaryStringData.fromString(title),
                category == null ? null : BinaryStringData.fromString(category),
                new GenericArrayData(
                        new float[] {
                            firstVectorValue, firstVectorValue + 1, firstVectorValue + 2
                        }));
    }

    private static Map<String, Object> awaitEntity(String collectionName, Object id) {
        final Map<String, Object>[] entity = new Map[1];
        flushAndLoadCollection(collectionName);
        Awaitility.await("Milvus entity " + collectionName + "/" + id)
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(
                        () -> {
                            Map<String, Object> result = getEntity(collectionName, id);
                            Assertions.assertThat(result).isNotNull();
                            entity[0] = result;
                        });
        return entity[0];
    }

    private static void awaitEntityAbsent(String collectionName, Object id) {
        flushAndLoadCollection(collectionName);
        Awaitility.await("Milvus entity removed " + collectionName + "/" + id)
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> Assertions.assertThat(getEntity(collectionName, id)).isNull());
    }

    private static void flushAndLoadCollection(String collectionName) {
        MilvusClientV2 client = MilvusClientUtils.createClient(currentConfig());
        try {
            client.flush(
                    FlushReq.builder()
                            .databaseName("default")
                            .collectionNames(Collections.singletonList(collectionName))
                            .waitFlushedTimeoutMs(10_000L)
                            .build());
            client.loadCollection(
                    LoadCollectionReq.builder()
                            .databaseName("default")
                            .collectionName(collectionName)
                            .sync(true)
                            .timeout(30_000L)
                            .build());
        } finally {
            client.close();
        }
    }

    private static Map<String, Object> getEntity(String collectionName, Object id) {
        MilvusClientV2 client = MilvusClientUtils.createClient(currentConfig());
        try {
            GetResp response =
                    client.get(
                            GetReq.builder()
                                    .databaseName("default")
                                    .collectionName(collectionName)
                                    .ids(Collections.singletonList(id))
                                    .outputFields(Arrays.asList("*"))
                                    .build());
            List<QueryResp.QueryResult> results = response.getGetResults();
            if (results == null || results.isEmpty()) {
                return null;
            }
            return results.get(0).getEntity();
        } finally {
            client.close();
        }
    }

    private static DescribeCollectionResp describeCollection(String collectionName) {
        MilvusClientV2 client = MilvusClientUtils.createClient(currentConfig());
        try {
            return client.describeCollection(
                    DescribeCollectionReq.builder()
                            .databaseName("default")
                            .collectionName(collectionName)
                            .build());
        } finally {
            client.close();
        }
    }

    private static boolean hasPartition(String collectionName, String partitionName) {
        MilvusClientV2 client = MilvusClientUtils.createClient(currentConfig());
        try {
            return client.hasPartition(
                    HasPartitionReq.builder()
                            .databaseName("default")
                            .collectionName(collectionName)
                            .partitionName(partitionName)
                            .build());
        } finally {
            client.close();
        }
    }

    private static boolean isCollectionLoaded(String collectionName) {
        MilvusClientV2 client = MilvusClientUtils.createClient(currentConfig());
        try {
            return client.getLoadState(
                    GetLoadStateReq.builder()
                            .databaseName("default")
                            .collectionName(collectionName)
                            .build());
        } finally {
            client.close();
        }
    }

    private static void dropCollectionIfExists(String collectionName) {
        MilvusClientV2 client = MilvusClientUtils.createClient(currentConfig());
        try {
            boolean exists =
                    client.hasCollection(
                            HasCollectionReq.builder()
                                    .databaseName("default")
                                    .collectionName(collectionName)
                                    .build());
            if (exists) {
                client.dropCollection(
                        DropCollectionReq.builder()
                                .databaseName("default")
                                .collectionName(collectionName)
                                .build());
            }
        } finally {
            client.close();
        }
    }

    private static MilvusDataSinkConfig currentConfig() {
        return config(
                milvusUri,
                Collections.emptyMap(),
                Collections.singletonList(MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                Collections.emptyMap(),
                100,
                Duration.ofSeconds(10),
                1,
                Duration.ofMillis(100),
                true,
                true,
                "strong");
    }

    private static MilvusDataSinkConfig allowPrimaryKeyChangeConfig() {
        return config(
                milvusUri,
                Collections.emptyMap(),
                Collections.singletonList(MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                Collections.emptyMap(),
                100,
                Duration.ofSeconds(10),
                1,
                Duration.ofMillis(100),
                true,
                true,
                "strong",
                false,
                "",
                false,
                Collections.emptyList(),
                "allow");
    }

    private static MilvusDataSinkConfig config(
            String uri,
            Map<TableId, String> collectionMappings,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled,
            boolean createIndexEnabled,
            String consistencyLevel) {
        return config(
                uri,
                collectionMappings,
                vectorFields,
                tableVectorFields,
                flushMaxRows,
                flushInterval,
                maxRetries,
                retryBackoff,
                deleteEnabled,
                createIndexEnabled,
                consistencyLevel,
                false,
                "",
                false,
                Collections.emptyList(),
                "reject");
    }

    private static MilvusDataSinkConfig config(
            String uri,
            Map<TableId, String> collectionMappings,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled,
            boolean createIndexEnabled,
            String consistencyLevel,
            boolean loadCollectionEnabled,
            String partitionField,
            boolean partitionAutoCreateEnabled,
            List<String> partitionNames) {
        return config(
                uri,
                collectionMappings,
                vectorFields,
                tableVectorFields,
                flushMaxRows,
                flushInterval,
                maxRetries,
                retryBackoff,
                deleteEnabled,
                createIndexEnabled,
                consistencyLevel,
                loadCollectionEnabled,
                partitionField,
                partitionAutoCreateEnabled,
                partitionNames,
                "reject");
    }

    private static MilvusDataSinkConfig config(
            String uri,
            Map<TableId, String> collectionMappings,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled,
            boolean createIndexEnabled,
            String consistencyLevel,
            boolean loadCollectionEnabled,
            String partitionField,
            boolean partitionAutoCreateEnabled,
            List<String> partitionNames,
            String primaryKeyChangeMode) {
        return new MilvusDataSinkConfig(
                uri,
                milvusToken == null ? "" : milvusToken,
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                collectionMappings,
                true,
                true,
                createIndexEnabled,
                false,
                loadCollectionEnabled,
                partitionField,
                partitionAutoCreateEnabled,
                1024,
                partitionNames,
                vectorFields,
                tableVectorFields,
                "",
                65535,
                flushMaxRows,
                flushInterval,
                maxRetries,
                retryBackoff,
                true,
                10,
                deleteEnabled,
                primaryKeyChangeMode,
                false,
                consistencyLevel,
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private static class MockContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    private static class CheckpointRestartSource extends RichParallelSourceFunction<Event>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private final TableId tableId;
        private final Schema schema;

        private transient BinaryRecordDataGenerator recordGenerator;
        private transient ListState<Integer> stageState;
        private volatile boolean running = true;
        private int stage;

        private CheckpointRestartSource(TableId tableId, Schema schema) {
            this.tableId = tableId;
            this.schema = schema;
        }

        @Override
        public void run(SourceFunction.SourceContext<Event> ctx) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            if (runtimeContext.getIndexOfThisSubtask() != 0) {
                waitUntilCancelled();
                return;
            }

            if (stage == 0) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new CreateTableEvent(tableId, schema));
                    ctx.collect(binaryInsert(20L, "doc-20-before-restart", 1.0f));
                    stage = 1;
                }
            }

            if (stage == 1 && RESTART_IT_FAILURE_TRIGGERED.get()) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new CreateTableEvent(tableId, schema));
                    ctx.collect(
                            DataChangeEvent.updateEvent(
                                    tableId,
                                    binaryRecord(20L, "doc-20-before-restart", 1.0f),
                                    binaryRecord(20L, "doc-20-after-restart", 4.0f)));
                    stage = 2;
                }
                return;
            }

            waitUntilCancelled();
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            stageState.clear();
            stageState.add(stage);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            stageState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "milvus-checkpoint-restart-stage", Integer.class));
            stage = 0;
            for (Integer restoredStage : stageState.get()) {
                stage = restoredStage;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (stage != 1 || !RESTART_IT_FAILURE_TRIGGERED.compareAndSet(false, true)) {
                return;
            }
            try {
                Assertions.assertThat(awaitEntity(DOCS_COLLECTION, 20L).get("title"))
                        .isEqualTo("doc-20-before-restart");
            } catch (Throwable t) {
                RESTART_IT_CHECKPOINT_ASSERTION_FAILURE.set(t);
            }
            throw new RuntimeException(
                    "Fail once after completed checkpoint " + checkpointId + " for restart test.");
        }

        private void waitUntilCancelled() throws InterruptedException {
            while (running) {
                Thread.sleep(50L);
            }
        }

        private DataChangeEvent binaryInsert(long id, String title, float firstVectorValue) {
            return DataChangeEvent.insertEvent(tableId, binaryRecord(id, title, firstVectorValue));
        }

        private RecordData binaryRecord(long id, String title, float firstVectorValue) {
            if (recordGenerator == null) {
                recordGenerator =
                        new BinaryRecordDataGenerator(
                                schema.getColumnDataTypes()
                                        .toArray(
                                                new org.apache.flink.cdc.common.types.DataType[0]));
            }
            return recordGenerator.generate(
                    new Object[] {
                        id,
                        BinaryStringData.fromString(title),
                        new GenericArrayData(
                                new float[] {
                                    firstVectorValue, firstVectorValue + 1, firstVectorValue + 2
                                })
                    });
        }
    }
}
