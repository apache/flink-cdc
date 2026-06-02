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

package org.apache.flink.cdc.connectors.pgvector.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.pgvector.utils.PgVectorColumnSpec;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Integration tests for pgvector sink against PostgreSQL with pgvector extension. */
@Testcontainers(disabledWithoutDocker = true)
class PgVectorSinkITCase {

    private static final TableId DOCS_TABLE = TableId.parse("public.docs");
    private static final TableId TAGS_TABLE = TableId.parse("public.tags");

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("pgvector/pgvector:pg16")
                    .withDatabaseName("vector_db")
                    .withUsername("test")
                    .withPassword("test");

    private PgVectorDataSinkConfig sinkConfig;

    @BeforeEach
    void setUp() throws SQLException {
        sinkConfig = createSinkConfig(ImmutableMap.of("public.docs.embedding", "vector(3)"));
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS public.docs");
            statement.execute("DROP TABLE IF EXISTS public.tags");
        }
    }

    @Test
    void testVectorUpsertUpdateAndDelete() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.FLOAT())));

        PgVectorMetadataApplier metadataApplier = new PgVectorMetadataApplier(sinkConfig);
        metadataApplier.applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            generator.generate(
                                    new Object[] {
                                        1, new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})
                                    })),
                    new MockContext());
            writer.flush(false);
            Assertions.assertThat(queryEmbedding(1)).isEqualTo("[1,2,3]");

            writer.write(
                    DataChangeEvent.updateEvent(
                            DOCS_TABLE,
                            generator.generate(
                                    new Object[] {
                                        1, new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f})
                                    }),
                            generator.generate(
                                    new Object[] {
                                        1, new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f})
                                    })),
                    new MockContext());
            writer.flush(false);
            Assertions.assertThat(queryEmbedding(1)).isEqualTo("[4,5,6]");

            writer.write(
                    DataChangeEvent.deleteEvent(
                            DOCS_TABLE,
                            generator.generate(
                                    new Object[] {
                                        1, new GenericArrayData(new float[] {4.0f, 5.0f, 6.0f})
                                    })),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(queryCount("public.docs")).isEqualTo(0);
    }

    @Test
    void testPostgresArrayColumnWrite() throws Exception {
        PgVectorDataSinkConfig tagsConfig = createSinkConfig(ImmutableMap.of());
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("values", DataTypes.ARRAY(DataTypes.INT()))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT())));

        PgVectorMetadataApplier metadataApplier = new PgVectorMetadataApplier(tagsConfig);
        metadataApplier.applySchemaChange(new CreateTableEvent(TAGS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(tagsConfig)) {
            writer.write(new CreateTableEvent(TAGS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            TAGS_TABLE,
                            generator.generate(
                                    new Object[] {1, new GenericArrayData(new int[] {1, 2, 3})})),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(queryIntArray("public.tags", "values", 1)).isEqualTo("{1,2,3}");
    }

    @Test
    void testHalfvecWrite() throws Exception {
        PgVectorDataSinkConfig halfvecConfig =
                createSinkConfig(ImmutableMap.of("public.docs.embedding", "halfvec(2)"));
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.FLOAT())));

        new PgVectorMetadataApplier(halfvecConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(halfvecConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            generator.generate(
                                    new Object[] {
                                        1, new GenericArrayData(new float[] {1.0f, 2.0f})
                                    })),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(
                        querySingleValue("SELECT embedding::text FROM public.docs WHERE id = 1"))
                .isEqualTo("[1,2]");
    }

    @Test
    void testSparsevecWrite() throws Exception {
        PgVectorDataSinkConfig sparsevecConfig =
                createSinkConfig(ImmutableMap.of("public.docs.embedding", "sparsevec(5)"));
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.FLOAT())));

        new PgVectorMetadataApplier(sparsevecConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(sparsevecConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            generator.generate(
                                    new Object[] {
                                        1,
                                        new GenericArrayData(
                                                new float[] {1.0f, 0.0f, 2.0f, 0.0f, 3.0f})
                                    })),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(
                        querySingleValue("SELECT embedding::text FROM public.docs WHERE id = 1"))
                .isEqualTo("{1:1,3:2,5:3}/5");
    }

    @Test
    void testBitVectorWrite() throws Exception {
        PgVectorDataSinkConfig bitConfig =
                createSinkConfig(ImmutableMap.of("public.docs.embedding", "bit(4)"));
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("embedding", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.STRING()));

        new PgVectorMetadataApplier(bitConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(bitConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            generator.generate(
                                    new Object[] {1, BinaryStringData.fromString("1010")})),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(
                        querySingleValue("SELECT embedding::text FROM public.docs WHERE id = 1"))
                .isEqualTo("1010");
    }

    @Test
    void testDropTableRemovesWriterSchema() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT()));

        new PgVectorMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(new DropTableEvent(DOCS_TABLE), new MockContext());
            Assertions.assertThatThrownBy(
                            () ->
                                    writer.write(
                                            DataChangeEvent.insertEvent(
                                                    DOCS_TABLE,
                                                    generator.generate(new Object[] {1})),
                                            new MockContext()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Schema for table");
        }
    }

    @Test
    void testAddColumnAndWrite() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator initialGenerator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT()));

        new PgVectorMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        BinaryRecordDataGenerator extendedGenerator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT(), DataTypes.STRING()));
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        DOCS_TABLE,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("title", DataTypes.STRING()))));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE, initialGenerator.generate(new Object[] {1})),
                    new MockContext());
            writer.flush(false);

            new PgVectorMetadataApplier(sinkConfig).applySchemaChange(addColumnEvent);
            writer.write(addColumnEvent, new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(
                            DOCS_TABLE,
                            extendedGenerator.generate(
                                    new Object[] {2, BinaryStringData.fromString("hello")})),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(querySingleValue("SELECT title FROM public.docs WHERE id = 2"))
                .isEqualTo("hello");
    }

    @Test
    void testTruncateKeepsWriterSchemaUsable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.INT()));
        TruncateTableEvent truncateTableEvent = new TruncateTableEvent(DOCS_TABLE);

        new PgVectorMetadataApplier(sinkConfig)
                .applySchemaChange(new CreateTableEvent(DOCS_TABLE, schema));

        try (PgVectorSinkWriter writer = new PgVectorSinkWriter(sinkConfig)) {
            writer.write(new CreateTableEvent(DOCS_TABLE, schema), new MockContext());
            writer.write(
                    DataChangeEvent.insertEvent(DOCS_TABLE, generator.generate(new Object[] {1})),
                    new MockContext());
            writer.flush(false);
            Assertions.assertThat(queryCount("public.docs")).isEqualTo(1);

            new PgVectorMetadataApplier(sinkConfig).applySchemaChange(truncateTableEvent);
            writer.write(truncateTableEvent, new MockContext());
            Assertions.assertThat(queryCount("public.docs")).isEqualTo(0);

            writer.write(
                    DataChangeEvent.insertEvent(DOCS_TABLE, generator.generate(new Object[] {2})),
                    new MockContext());
            writer.flush(false);
        }

        Assertions.assertThat(queryCount("public.docs")).isEqualTo(1);
    }

    private static PgVectorDataSinkConfig createSinkConfig(Map<String, String> vectorColumns) {
        Map<String, PgVectorColumnSpec> parsed = new HashMap<>();
        vectorColumns.forEach((key, value) -> parsed.put(key, PgVectorColumnSpec.parse(value)));

        return new PgVectorDataSinkConfig(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword(),
                "public",
                true,
                true,
                true,
                100,
                Duration.ofSeconds(10),
                3,
                Duration.ofSeconds(1),
                true,
                false,
                parsed,
                ImmutableMap.of());
    }

    private static Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static String queryEmbedding(int id) throws SQLException {
        return querySingleValue("SELECT embedding::text FROM public.docs WHERE id = " + id);
    }

    private static int queryCount(String tableName) throws SQLException {
        return Integer.parseInt(querySingleValue("SELECT COUNT(*) FROM " + tableName));
    }

    private static String querySingleValue(String sql) throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            Assertions.assertThat(resultSet.next()).isTrue();
            return resultSet.getString(1);
        }
    }

    private static String queryIntArray(String tableName, String columnName, int id)
            throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT " + columnName + " FROM " + tableName + " WHERE id = ?")) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assertions.assertThat(resultSet.next()).isTrue();
                Integer[] values = (Integer[]) resultSet.getArray(1).getArray();
                return IntStream.range(0, values.length)
                        .mapToObj(i -> values[i].toString())
                        .collect(Collectors.joining(",", "{", "}"));
            }
        }
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
}
