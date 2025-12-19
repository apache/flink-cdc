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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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

    @Test
    void testReadStreamSplitWithException() throws Exception {
        customDatabase.createAndInitialize();
        PostgresSourceConfigFactory sourceConfigFactory =
                getMockPostgresSourceConfigFactory(customDatabase, schemaName, tableName, 10, true);
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
