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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.MockPostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.RecordsFormatter;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.core.io.InputStatus.END_OF_INPUT;
import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSourceReader}. */
public class PostgresSourceReaderTest extends PostgresTestBase {
    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "customer";
    private String slotName;
    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @BeforeEach
    public void before() throws SQLException {
        customDatabase.createAndInitialize();

        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        customDatabase.removeSlot(slotName);
    }

    @Test
    void testNotifyCheckpointWindowSizeOne() throws Exception {
        final PostgresSourceReader reader = createReader(1);
        final List<Long> completedCheckpointIds = new ArrayList<>();
        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                id -> completedCheckpointIds.add(id));
        reader.notifyCheckpointComplete(11L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(12L);
        assertThat(completedCheckpointIds).containsExactly(11L);
        reader.notifyCheckpointComplete(13L);
        assertThat(completedCheckpointIds).containsExactly(11L, 12L);
    }

    @Test
    void testNotifyCheckpointWindowSizeDefault() throws Exception {
        final PostgresSourceReader reader = createReader(3);
        final List<Long> completedCheckpointIds = new ArrayList<>();
        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                id -> completedCheckpointIds.add(id));
        reader.notifyCheckpointComplete(103L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(102L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(101L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(104L);
        assertThat(completedCheckpointIds).containsExactly(101L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMultipleSnapshotSplit(boolean skipBackFill) throws Exception {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("Id", DataTypes.BIGINT()),
                        DataTypes.FIELD("Name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));

        TableId tableId = new TableId(null, SCHEMA_NAME, "Customers");
        RowType splitType =
                RowType.of(
                        new LogicalType[] {DataTypes.INT().getLogicalType()}, new String[] {"Id"});
        List<SnapshotSplit> snapshotSplits =
                Arrays.asList(
                        new SnapshotSplit(
                                tableId,
                                0,
                                splitType,
                                null,
                                new Integer[] {200},
                                null,
                                Collections.emptyMap()),
                        new SnapshotSplit(
                                tableId,
                                1,
                                splitType,
                                new Integer[] {200},
                                new Integer[] {1500},
                                null,
                                Collections.emptyMap()),
                        new SnapshotSplit(
                                tableId,
                                2,
                                splitType,
                                new Integer[] {1500},
                                null,
                                null,
                                Collections.emptyMap()));

        // Step 1: start source reader and assign snapshot splits
        PostgresSourceReader reader = createReader(-1, skipBackFill);
        reader.start();
        reader.addSplits(snapshotSplits);

        String[] expectedRecords =
                new String[] {
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        // Step 2: wait the snapshot splits finished reading
        List<String> actualRecords = consumeSnapshotRecords(reader, dataType);
        assertEqualsInAnyOrder(Arrays.asList(expectedRecords), actualRecords);
    }

    private List<String> consumeSnapshotRecords(
            PostgresSourceReader sourceReader, DataType recordType) throws Exception {
        // Poll all the  records of the multiple assigned snapshot split.
        sourceReader.notifyNoMoreSplits();
        final SimpleReaderOutput output = new SimpleReaderOutput();
        InputStatus status = MORE_AVAILABLE;
        while (END_OF_INPUT != status) {
            status = sourceReader.pollNext(output);
        }
        final RecordsFormatter formatter = new RecordsFormatter(recordType);
        return formatter.format(output.getResults());
    }

    private PostgresSourceReader createReader(final int lsnCommitCheckpointsDelay)
            throws Exception {
        return createReader(lsnCommitCheckpointsDelay, false);
    }

    private PostgresSourceReader createReader(
            final int lsnCommitCheckpointsDelay, boolean skipBackFill) throws Exception {
        final PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        final PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname(customDatabase.getHost());
        configFactory.port(customDatabase.getDatabasePort());
        configFactory.database(customDatabase.getDatabaseName());
        configFactory.tableList(SCHEMA_NAME + ".Customers");
        configFactory.username(customDatabase.getUsername());
        configFactory.password(customDatabase.getPassword());
        configFactory.setLsnCommitCheckpointsDelay(lsnCommitCheckpointsDelay);
        configFactory.skipSnapshotBackfill(skipBackFill);
        configFactory.decodingPluginName("pgoutput");
        MockPostgresDialect dialect = new MockPostgresDialect(configFactory.create(0));
        final PostgresSourceBuilder.PostgresIncrementalSource<?> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory, new ForwardDeserializeSchema(), offsetFactory, dialect);
        return source.createReader(new TestingReaderContext());
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------
    private static class SimpleReaderOutput implements ReaderOutput<SourceRecord> {

        private final List<SourceRecord> results = new ArrayList<>();

        @Override
        public void collect(SourceRecord record) {
            results.add(record);
        }

        public List<SourceRecord> getResults() {
            return results;
        }

        @Override
        public void collect(SourceRecord record, long timestamp) {
            collect(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceOutput<SourceRecord> createOutputForSplit(java.lang.String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(java.lang.String splitId) {}
    }

    private static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) throws Exception {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }
}
