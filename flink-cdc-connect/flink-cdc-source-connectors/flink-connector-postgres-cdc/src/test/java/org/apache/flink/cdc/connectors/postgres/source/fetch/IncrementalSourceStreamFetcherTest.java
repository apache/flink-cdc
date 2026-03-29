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

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link IncrementalSourceStreamFetcher }. */
public class IncrementalSourceStreamFetcherTest extends PostgresTestBase {

    private static final String schemaName = "customer";
    private static final String tableName = "Customers";

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    @BeforeEach
    public void before() throws SQLException {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        // sleep 1000ms to wait until connections are closed.
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    @Test
    void testReadStreamSplitWithException() throws Exception {
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, slotName, 10, true);
        sourceConfigFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(sourceConfigFactory.create(0));

        // Create reader and submit splits
        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        IncrementalSourceStreamFetcher fetcher = new IncrementalSourceStreamFetcher(taskContext, 0);
        StreamSplit split = createStreamSplit(sourceConfig, dialect);
        PostgresStreamFetchTask fetchTask =
                (PostgresStreamFetchTask) dialect.createFetchTask(split);
        StoppableChangeEventSourceContext changeEventSourceContext =
                fetchTask.getChangeEventSourceContext();

        fetcher.submitTask(fetchTask);
        // Mock an exception occurring during stream split reading by setting the error handler
        // and stopping the change event source to test exception handling
        taskContext
                .getErrorHandler()
                .setProducerThrowable(new RuntimeException("Test read with exception"));
        changeEventSourceContext.stopChangeEventSource();

        // Wait for the task to complete
        Thread.sleep(500L);

        assertThatThrownBy(
                        () -> pollRecordsFromReader(fetcher, SourceRecordUtils::isDataChangeRecord))
                .rootCause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("Test read with exception");
        fetcher.close();
    }

    @Test
    void testSchemaChangeRecordInQueue() throws Exception {
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(
                        customDatabase, schemaName, tableName, slotName, 10, true);
        sourceConfigFactory.startupOptions(StartupOptions.latest());
        PostgresSourceConfig sourceConfig = sourceConfigFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(sourceConfigFactory.create(0));

        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        IncrementalSourceStreamFetcher fetcher = new IncrementalSourceStreamFetcher(taskContext, 0);
        StreamSplit split = createStreamSplit(sourceConfig, dialect);
        PostgresStreamFetchTask fetchTask =
                (PostgresStreamFetchTask) dialect.createFetchTask(split);

        fetcher.submitTask(fetchTask);
        // Wait for the stream reader to start consuming WAL
        Thread.sleep(1000L);

        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, customDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            // Insert a record BEFORE the DDL change
            stmt.execute(
                    "INSERT INTO customer.\"Customers\" VALUES (3001, 'before_ddl', 'Beijing', '111')");

            // Perform a DDL change to trigger schema change event
            stmt.execute(
                    "ALTER TABLE customer.\"Customers\" ADD COLUMN email VARCHAR(255) DEFAULT 'test@test.com'");

            // Insert a record AFTER the DDL change
            stmt.execute(
                    "INSERT INTO customer.\"Customers\" VALUES (3002, 'after_ddl', 'Shanghai', '222', 'after@test.com')");
        }

        // Wait for all events to be enqueued
        Thread.sleep(1000L);

        // Poll all records (data changes + schema changes) to verify ordering
        List<SourceRecord> allRecords = pollRecordsFromReader(fetcher, r -> !isHeartbeatEvent(r));
        assertThat(allRecords).hasSize(4);
        assertThat(allRecords.get(0).toString())
                .contains(
                        "PostgresSchemaRecord{table=columns: {\n"
                                + "  Id int4(10, 0) NOT NULL\n"
                                + "  Name varchar(255, 0) NOT NULL DEFAULT VALUE 'flink'::character varying\n"
                                + "  address varchar(1024, 0) DEFAULT VALUE NULL\n"
                                + "  phone_number varchar(512, 0) DEFAULT VALUE NULL\n"
                                + "}\n"
                                + "primary key: [Id]\n"
                                + "default charset: null\n"
                                + "comment: null\n"
                                + "}");
        assertThat(allRecords.get(1).toString()).contains("before_ddl");
        assertThat(allRecords.get(2).toString())
                .contains(
                        "PostgresSchemaRecord{table=columns: {\n"
                                + "  Id int4(10, 0) NOT NULL\n"
                                + "  Name varchar(255, 0) NOT NULL DEFAULT VALUE 'flink'::character varying\n"
                                + "  address varchar(1024, 0) DEFAULT VALUE NULL\n"
                                + "  phone_number varchar(512, 0) DEFAULT VALUE NULL\n"
                                + "  email varchar(255, 0) DEFAULT VALUE 'test@test.com'::character varying\n"
                                + "}\n"
                                + "primary key: [Id]\n"
                                + "default charset: null\n"
                                + "comment: null\n"
                                + "}");
        assertThat(allRecords.get(3).toString()).contains("after_ddl");

        fetcher.close();
    }

    private StreamSplit createStreamSplit(
            PostgresSourceConfig sourceConfig, PostgresDialect dialect) throws Exception {
        StreamSplitAssigner streamSplitAssigner =
                new StreamSplitAssigner(
                        sourceConfig,
                        dialect,
                        new PostgresOffsetFactory(),
                        new MockSplitEnumeratorContext<>(1));
        streamSplitAssigner.open();

        Map<TableId, TableChanges.TableChange> tableSchemas =
                dialect.discoverDataCollectionSchemas(sourceConfig);
        return StreamSplit.fillTableSchemas(streamSplitAssigner.createStreamSplit(), tableSchemas);
    }

    private List<SourceRecord> pollRecordsFromReader(
            IncrementalSourceStreamFetcher fetcher, Predicate<SourceRecord> filter) {
        List<SourceRecord> records = new ArrayList<>();
        Iterator<SourceRecords> recordIterator;
        try {
            recordIterator = fetcher.pollSplitRecords();
        } catch (InterruptedException e) {
            throw new RuntimeException("Polling action was interrupted", e);
        }
        if (recordIterator == null) {
            return records;
        }
        while (recordIterator.hasNext()) {
            Iterator<SourceRecord> iterator = recordIterator.next().iterator();
            while (iterator.hasNext()) {
                SourceRecord record = iterator.next();
                if (filter.test(record)) {
                    records.add(record);
                }
            }
        }
        LOG.debug("Records polled: {}", records);
        return records;
    }
}
