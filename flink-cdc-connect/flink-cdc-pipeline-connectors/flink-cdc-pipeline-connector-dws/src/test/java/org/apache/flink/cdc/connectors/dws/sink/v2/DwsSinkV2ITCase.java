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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.data.GenericMapData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.dws.sink.DwsMetadataApplier;
import org.apache.flink.cdc.connectors.dws.utils.DwsContainer;
import org.apache.flink.cdc.connectors.dws.utils.DwsSinkTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the DWS SinkV2 staging-table commit path. */
class DwsSinkV2ITCase extends DwsSinkTestBase {

    private static final String TABLE_NAME = "sink_v2_2pc_test";
    private static final TableId TABLE_ID = TableId.tableId(DwsContainer.DWS_SCHEMA, TABLE_NAME);

    @BeforeEach
    public void initializeDatabase() {
        createDatabase(DwsContainer.DWS_DATABASE_TEST);
        createSchema(DwsContainer.DWS_DATABASE_TEST, DwsContainer.DWS_SCHEMA);
        dropTable(TABLE_ID);
        dropTable(TableId.tableId(DwsContainer.DWS_SCHEMA, DwsSqlUtils.COMMIT_TABLE));
    }

    @AfterEach
    public void destroyDatabase() {
        dropDatabase(DwsContainer.DWS_DATABASE_TEST);
    }

    @Test
    void testCommitStagedRowsAndRetryIdempotently() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(32))
                        .physicalColumn("amount", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, schema);
        applySchemaChange(createTableEvent);

        DwsWriter writer =
                new DwsWriter(
                        DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST),
                        DWS_CONTAINER.getUsername(),
                        DWS_CONTAINER.getPassword(),
                        ZoneId.of("UTC"),
                        false,
                        DwsContainer.DWS_SCHEMA,
                        true,
                        "test-job",
                        0,
                        CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);

        Collection<DwsCommittable> committables;
        try {
            writer.write(createTableEvent, null);
            for (DataChangeEvent event : createDataChangeEvents()) {
                writer.write(event, null);
            }
            committables = writer.prepareCommit();
        } finally {
            writer.close();
        }

        assertThat(committables).hasSize(1);
        DwsCommittable committable = committables.iterator().next();
        assertThat(tableExists(committable.getStagingSchema(), committable.getStagingTable()))
                .isTrue();

        DwsCommitter committer =
                new DwsCommitter(
                        DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST),
                        DWS_CONTAINER.getUsername(),
                        DWS_CONTAINER.getPassword(),
                        DwsContainer.DWS_SCHEMA,
                        false);
        try {
            TestingCommitRequest firstCommit = new TestingCommitRequest(committable);
            committer.commit(Collections.singletonList(firstCommit));
            assertThat(firstCommit.alreadyCommitted).isTrue();
            assertThat(firstCommit.retryLater).isFalse();

            assertThat(fetchTargetRows())
                    .containsExactly("1 | Alice-updated | 11", "3 | Carol | 30");
            assertThat(tableExists(committable.getStagingSchema(), committable.getStagingTable()))
                    .isFalse();
            assertThat(fetchCommitMarkerCount(committable)).isEqualTo(1);

            TestingCommitRequest retryCommit = new TestingCommitRequest(committable);
            committer.commit(Collections.singletonList(retryCommit));
            assertThat(retryCommit.alreadyCommitted).isTrue();
            assertThat(retryCommit.retryLater).isFalse();
            assertThat(fetchTargetRows())
                    .containsExactly("1 | Alice-updated | 11", "3 | Carol | 30");
            assertThat(fetchCommitMarkerCount(committable)).isEqualTo(1);
        } finally {
            committer.close();
        }
    }

    @Test
    void testFlushEventCommitsRowsBeforeDestructiveSchemaChange() throws Exception {
        Schema originalSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(32))
                        .physicalColumn("amount", DataTypes.INT())
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, originalSchema);
        applySchemaChange(createTableEvent);

        DwsWriter writer = createWriter("schema-flush-job");
        try {
            writer.write(createTableEvent, null);
            writer.write(createInsertEvent(originalSchema, 1, "Alice", 10), null);

            writer.flush(
                    new FlushEvent(
                            0,
                            Collections.singletonList(TABLE_ID),
                            SchemaChangeEventType.DROP_COLUMN));

            assertThat(fetchRows("id, name, amount")).containsExactly("1 | Alice | 10");

            DropColumnEvent dropColumnEvent =
                    new DropColumnEvent(TABLE_ID, Collections.singletonList("amount"));
            applySchemaChange(dropColumnEvent);
            writer.write(dropColumnEvent, null);

            Schema evolvedSchema =
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT().notNull())
                            .physicalColumn("name", DataTypes.VARCHAR(32))
                            .primaryKey("id")
                            .build();
            writer.write(createInsertEvent(evolvedSchema, 2, "Bob"), null);

            Collection<DwsCommittable> committables = writer.prepareCommit();
            assertThat(committables).hasSize(1);
            commit(committables);
            assertThat(fetchRows("id, name")).containsExactly("1 | Alice", "2 | Bob");
        } finally {
            writer.close();
        }
    }

    @Test
    void testFlushEventCommitIsIdempotentAfterRecoveryReplay() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(32))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, schema);
        applySchemaChange(createTableEvent);
        FlushEvent flushEvent =
                new FlushEvent(
                        0, Collections.singletonList(TABLE_ID), SchemaChangeEventType.ADD_COLUMN);

        DwsWriter firstAttempt = createWriter("replay-flush-job");
        try {
            firstAttempt.write(createTableEvent, null);
            firstAttempt.write(createInsertEvent(schema, 1, "Alice"), null);
            firstAttempt.flush(flushEvent);
        } finally {
            firstAttempt.close();
        }

        DwsWriter replayAttempt = createWriter("replay-flush-job");
        try {
            replayAttempt.write(createTableEvent, null);
            replayAttempt.write(createInsertEvent(schema, 1, "Alice"), null);
            replayAttempt.flush(flushEvent);
            assertThat(fetchRows("id, name")).containsExactly("1 | Alice");
            assertThat(replayAttempt.prepareCommit()).isEmpty();
        } finally {
            replayAttempt.close();
        }
    }

    @Test
    void testCommitComplexTypesIntoRealDwsTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("tags", DataTypes.ARRAY(DataTypes.STRING()))
                        .physicalColumn(
                                "attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .physicalColumn(
                                "payload",
                                DataTypes.ROW(
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("count", DataTypes.INT())))
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, schema);
        applySchemaChange(createTableEvent);

        Map<Object, Object> attributes = new LinkedHashMap<>();
        attributes.put(BinaryStringData.fromString("k1"), BinaryStringData.fromString("v1"));
        attributes.put(BinaryStringData.fromString("k2"), BinaryStringData.fromString("v2"));

        DwsWriter writer = createWriter("complex-types-job");
        try {
            writer.write(createTableEvent, null);
            writer.write(
                    createInsertEvent(
                            schema,
                            1,
                            new GenericArrayData(
                                    new Object[] {
                                        BinaryStringData.fromString("alpha"),
                                        BinaryStringData.fromString("beta")
                                    }),
                            new GenericMapData(attributes),
                            GenericRecordData.of(BinaryStringData.fromString("nested"), 7)),
                    null);

            Collection<DwsCommittable> committables = writer.prepareCommit();
            assertThat(committables).hasSize(1);
            commit(committables);

            assertThat(fetchRows("id, tags, attributes::text, payload::text"))
                    .singleElement()
                    .satisfies(
                            row ->
                                    assertThat(row)
                                            .contains("1 | [\"alpha\",\"beta\"]")
                                            .contains("\"k1\"")
                                            .contains("\"v1\"")
                                            .contains("\"k2\"")
                                            .contains("\"v2\"")
                                            .contains("\"name\"")
                                            .contains("\"nested\"")
                                            .contains("\"count\"")
                                            .contains("7"));
        } finally {
            writer.close();
        }
    }

    private void applySchemaChange(CreateTableEvent createTableEvent) {
        new DwsMetadataApplier(
                        DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST),
                        DWS_CONTAINER.getUsername(),
                        DWS_CONTAINER.getPassword(),
                        false,
                        DwsContainer.DWS_SCHEMA,
                        false,
                        null)
                .applySchemaChange(createTableEvent);
    }

    private void applySchemaChange(DropColumnEvent dropColumnEvent) {
        new DwsMetadataApplier(
                        DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST),
                        DWS_CONTAINER.getUsername(),
                        DWS_CONTAINER.getPassword(),
                        false,
                        DwsContainer.DWS_SCHEMA,
                        false,
                        null)
                .applySchemaChange(dropColumnEvent);
    }

    private DwsWriter createWriter(String jobId) {
        return new DwsWriter(
                DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST),
                DWS_CONTAINER.getUsername(),
                DWS_CONTAINER.getPassword(),
                ZoneId.of("UTC"),
                false,
                DwsContainer.DWS_SCHEMA,
                true,
                jobId,
                0,
                CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
    }

    private DataChangeEvent createInsertEvent(Schema schema, Object... values) {
        return DataChangeEvent.insertEvent(
                TABLE_ID,
                new BinaryRecordDataGenerator((RowType) schema.toRowDataType())
                        .generate(toInternalValues(values)));
    }

    private Object[] toInternalValues(Object[] values) {
        Object[] converted = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            converted[i] =
                    values[i] instanceof String
                            ? BinaryStringData.fromString((String) values[i])
                            : values[i];
        }
        return converted;
    }

    private void commit(Collection<DwsCommittable> committables) throws Exception {
        DwsCommitter committer =
                new DwsCommitter(
                        DWS_CONTAINER.getJdbcUrl(DwsContainer.DWS_DATABASE_TEST),
                        DWS_CONTAINER.getUsername(),
                        DWS_CONTAINER.getPassword(),
                        DwsContainer.DWS_SCHEMA,
                        false);
        try {
            List<Committer.CommitRequest<DwsCommittable>> requests = new ArrayList<>();
            List<TestingCommitRequest> testingRequests = new ArrayList<>();
            for (DwsCommittable committable : committables) {
                TestingCommitRequest request = new TestingCommitRequest(committable);
                requests.add(request);
                testingRequests.add(request);
            }
            committer.commit(requests);
            assertThat(testingRequests)
                    .allSatisfy(
                            request -> {
                                assertThat(request.alreadyCommitted).isTrue();
                                assertThat(request.retryLater).isFalse();
                            });
        } finally {
            committer.close();
        }
    }

    private List<DataChangeEvent> createDataChangeEvents() {
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.VARCHAR(32), DataTypes.INT()));

        return Arrays.asList(
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        generator.generate(
                                new Object[] {
                                    1, BinaryStringData.fromString("Alice"), 10,
                                })),
                DataChangeEvent.updateEvent(
                        TABLE_ID,
                        generator.generate(
                                new Object[] {
                                    1, BinaryStringData.fromString("Alice"), 10,
                                }),
                        generator.generate(
                                new Object[] {
                                    1, BinaryStringData.fromString("Alice-updated"), 11,
                                })),
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        generator.generate(
                                new Object[] {
                                    2, BinaryStringData.fromString("Bob"), 20,
                                })),
                DataChangeEvent.deleteEvent(
                        TABLE_ID,
                        generator.generate(
                                new Object[] {
                                    2, BinaryStringData.fromString("Bob"), 20,
                                })),
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        generator.generate(
                                new Object[] {
                                    3, BinaryStringData.fromString("Carol"), 30,
                                })));
    }

    private List<String> fetchTargetRows() throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = createDatabaseConnection(DwsContainer.DWS_DATABASE_TEST);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT id, name, amount FROM "
                                        + DwsContainer.DWS_SCHEMA
                                        + "."
                                        + TABLE_NAME
                                        + " ORDER BY id")) {
            while (resultSet.next()) {
                rows.add(
                        resultSet.getString(1)
                                + " | "
                                + resultSet.getString(2)
                                + " | "
                                + resultSet.getString(3));
            }
        }
        return rows;
    }

    private List<String> fetchRows(String columns) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection connection = createDatabaseConnection(DwsContainer.DWS_DATABASE_TEST);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT "
                                        + columns
                                        + " FROM "
                                        + DwsContainer.DWS_SCHEMA
                                        + "."
                                        + TABLE_NAME
                                        + " ORDER BY id")) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    values.add(resultSet.getString(i));
                }
                rows.add(String.join(" | ", values));
            }
        }
        return rows;
    }

    private boolean tableExists(String schemaName, String tableName) throws SQLException {
        try (Connection connection = createDatabaseConnection(DwsContainer.DWS_DATABASE_TEST);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM information_schema.tables "
                                        + "WHERE table_schema = '"
                                        + schemaName
                                        + "' AND table_name = '"
                                        + tableName
                                        + "'")) {
            resultSet.next();
            return resultSet.getInt(1) > 0;
        }
    }

    private int fetchCommitMarkerCount(DwsCommittable committable) throws SQLException {
        try (Connection connection = createDatabaseConnection(DwsContainer.DWS_DATABASE_TEST);
                Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM "
                                        + DwsContainer.DWS_SCHEMA
                                        + "."
                                        + DwsSqlUtils.COMMIT_TABLE
                                        + " WHERE job_id = '"
                                        + committable.getJobId()
                                        + "' AND checkpoint_id = "
                                        + committable.getCheckpointId()
                                        + " AND subtask_id = "
                                        + committable.getSubtaskId()
                                        + " AND target_table = '"
                                        + committable.getTargetIdentifier()
                                        + "' AND staging_table = '"
                                        + committable.getStagingSchema()
                                        + "."
                                        + committable.getStagingTable()
                                        + "'")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private static final class TestingCommitRequest
            implements Committer.CommitRequest<DwsCommittable> {

        private DwsCommittable committable;
        private boolean alreadyCommitted;
        private boolean retryLater;

        private TestingCommitRequest(DwsCommittable committable) {
            this.committable = committable;
        }

        @Override
        public DwsCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable throwable) {
            throw new AssertionError(throwable);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable throwable) {
            throw new AssertionError(throwable);
        }

        @Override
        public void retryLater() {
            retryLater = true;
        }

        @Override
        public void updateAndRetryLater(DwsCommittable committable) {
            this.committable = committable;
            retryLater = true;
        }

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted = true;
        }
    }
}
