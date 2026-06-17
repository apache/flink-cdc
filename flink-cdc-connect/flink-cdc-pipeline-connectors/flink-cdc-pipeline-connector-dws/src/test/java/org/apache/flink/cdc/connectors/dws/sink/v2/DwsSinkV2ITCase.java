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
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
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
import java.util.List;

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
