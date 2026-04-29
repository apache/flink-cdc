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

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresStreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isHeartbeatEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link PostgresSourceStreamFetcher} verifying that {@code
 * pg_logical_emit_message} records reach the polled records when the feature flag is enabled and
 * are dropped when it is disabled.
 */
class PostgresSourceStreamFetcherITCase extends PostgresTestBase {

    private static final String SCHEMA_NAME = "customer";
    private static final String TABLE_NAME = "Customers";
    private static final long POLL_TIMEOUT_MS = 30_000L;

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    @BeforeEach
    void before() throws SQLException {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    void after() throws Exception {
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    @Test
    void logicalMessagesAreEmittedWhenEnabled() throws Exception {
        IncrementalSourceStreamFetcher fetcher = startFetcher(true);
        try {
            emitLogicalMessageAndDataChange("cdc-test", "hello");

            List<SourceRecord> records =
                    pollUntil(
                            fetcher,
                            r -> !isHeartbeatEvent(r),
                            recs -> recs.stream().anyMatch(this::isLogicalMessage));

            assertThat(records).anyMatch(this::isLogicalMessage);
        } finally {
            fetcher.close();
        }
    }

    @Test
    void logicalMessagesAreDroppedWhenDisabled() throws Exception {
        IncrementalSourceStreamFetcher fetcher = startFetcher(false);
        try {
            emitLogicalMessageAndDataChange("cdc-test", "hello");

            // Wait until the trailing INSERT is observed; that proves the fetcher has caught up
            // past the logical message, which should have been filtered out.
            List<SourceRecord> records =
                    pollUntil(
                            fetcher,
                            r -> !isHeartbeatEvent(r),
                            recs ->
                                    recs.stream()
                                            .anyMatch(
                                                    r ->
                                                            r.toString()
                                                                    .contains(
                                                                            "logical-msg-marker")));

            assertThat(records).noneMatch(this::isLogicalMessage);
        } finally {
            fetcher.close();
        }
    }

    private IncrementalSourceStreamFetcher startFetcher(boolean includeLogicalMessages)
            throws Exception {
        PostgresSourceConfigFactory factory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, SCHEMA_NAME, TABLE_NAME, slotName, 10, true);
        factory.startupOptions(StartupOptions.latest());
        factory.setIncludeLogicalMessages(includeLogicalMessages);

        PostgresSourceConfig sourceConfig = factory.create(0);
        PostgresDialect dialect = new PostgresDialect(factory.create(0));
        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);

        IncrementalSourceStreamFetcher fetcher = dialect.createStreamFetcher(taskContext, 0);
        StreamSplit split = createStreamSplit(sourceConfig, dialect);
        PostgresStreamFetchTask fetchTask =
                (PostgresStreamFetchTask) dialect.createFetchTask(split);
        fetcher.submitTask(fetchTask);

        // give the stream reader a moment to start consuming WAL
        Thread.sleep(1000L);
        return fetcher;
    }

    private void emitLogicalMessageAndDataChange(String prefix, String payload)
            throws SQLException {
        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, customDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    String.format(
                            "SELECT pg_logical_emit_message(false, '%s', '%s')", prefix, payload));
            // A trailing INSERT acts as a marker so we can wait for the WAL to advance past the
            // logical message even when it would be filtered out.
            stmt.execute(
                    "INSERT INTO customer.\"Customers\" VALUES "
                            + "(9001, 'logical-msg-marker', 'Beijing', '111')");
        }
    }

    private boolean isLogicalMessage(SourceRecord record) {
        if (!(record.value() instanceof Struct)) {
            return false;
        }
        Struct struct = (Struct) record.value();
        return struct.schema().field(Envelope.FieldName.OPERATION) != null
                && "m".equals(struct.getString(Envelope.FieldName.OPERATION));
    }

    private List<SourceRecord> pollUntil(
            IncrementalSourceStreamFetcher fetcher,
            Predicate<SourceRecord> keep,
            Predicate<List<SourceRecord>> done)
            throws Exception {
        List<SourceRecord> all = new ArrayList<>();
        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            Iterator<SourceRecords> batch = fetcher.pollSplitRecords();
            if (batch != null) {
                while (batch.hasNext()) {
                    Iterator<SourceRecord> it = batch.next().iterator();
                    while (it.hasNext()) {
                        SourceRecord r = it.next();
                        if (keep.test(r)) {
                            all.add(r);
                        }
                    }
                }
            }
            if (done.test(all)) {
                return all;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Timed out waiting for expected records. Polled so far: " + all);
    }

    private StreamSplit createStreamSplit(
            PostgresSourceConfig sourceConfig, PostgresDialect dialect) throws Exception {
        StreamSplitAssigner assigner =
                new StreamSplitAssigner(
                        sourceConfig,
                        dialect,
                        new PostgresOffsetFactory(),
                        new MockSplitEnumeratorContext<>(1));
        assigner.open();

        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(sourceConfig);
        return StreamSplit.fillTableSchemas(assigner.createStreamSplit(), tableSchemas);
    }
}
