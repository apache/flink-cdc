/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.reader;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;

import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.SampleBucketSplitStrategy;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.ShardedSplitStrategy;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.SingleSplitStrategy;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.SplitContext;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.SplitStrategy;
import com.ververica.cdc.connectors.mongodb.source.assigners.splitters.SplitVectorSplitStrategy;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;

import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** MongoDB snapshot split reader test case. */
public class MongoDBSnapshotSplitReaderTest extends MongoDBSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotSplitReaderTest.class);

    private static final int MAX_RETRY_TIMES = 100;

    private String database;

    private MongoDBSourceConfig sourceConfig;

    private MongoDBDialect dialect;

    private SplitContext splitContext;

    @Before
    public void before() {
        database = ROUTER.executeCommandFileInSeparateDatabase("chunk_test");

        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts(ROUTER.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + ".shopping_cart")
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .splitSizeMB(1)
                        .pollAwaitTimeMillis(500);

        sourceConfig = configFactory.create(0);

        dialect = new MongoDBDialect();

        splitContext = SplitContext.of(sourceConfig, new TableId(database, null, "shopping_cart"));
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithShardedSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(ShardedSplitStrategy.INSTANCE);
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithSplitVectorSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(SplitVectorSplitStrategy.INSTANCE);
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithSamplerSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(SampleBucketSplitStrategy.INSTANCE);
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithSingleSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(SingleSplitStrategy.INSTANCE);
    }

    private void testMongoDBSnapshotSplitReader(SplitStrategy splitter) throws Exception {
        LinkedList<SnapshotSplit> snapshotSplits = new LinkedList<>(splitter.split(splitContext));
        assertTrue(snapshotSplits.size() > 0);

        IncrementalSourceSplitReader<MongoDBSourceConfig> snapshotSplitReader =
                new IncrementalSourceSplitReader<>(0, dialect, sourceConfig);

        int retry = 0;
        long actualCount = 0;
        try {
            while (retry < MAX_RETRY_TIMES) {
                if (!snapshotSplits.isEmpty() && snapshotSplitReader.canAssignNextSplit()) {
                    SnapshotSplit snapshotSplit = snapshotSplits.poll();
                    LOG.info("Add snapshot split {}", snapshotSplit.splitId());
                    snapshotSplitReader.handleSplitsChanges(
                            new SplitsAddition<>(singletonList(snapshotSplit)));
                }

                ChangeEventRecords records = (ChangeEventRecords) snapshotSplitReader.fetch();
                if (records.nextSplit() != null) {
                    SourceRecords sourceRecords;
                    while ((sourceRecords = records.nextRecordFromSplit()) != null) {
                        Iterator<SourceRecord> iterator = sourceRecords.iterator();
                        while (iterator.hasNext()) {
                            SourceRecord record = iterator.next();
                            if (!isWatermarkEvent(record)) {
                                Struct value = (Struct) record.value();
                                BsonDocument fullDocument =
                                        BsonDocument.parse(value.getString(FULL_DOCUMENT_FIELD));
                                long productNo = fullDocument.getInt64("product_no").longValue();
                                String productKind =
                                        fullDocument.getString("product_kind").getValue();
                                String userId = fullDocument.getString("user_id").getValue();
                                String description =
                                        fullDocument.getString("description").getValue();

                                assertEquals("KIND_" + productNo, productKind);
                                assertEquals("user_" + productNo, userId);
                                assertEquals("my shopping cart " + productNo, description);
                                actualCount++;
                            }
                        }
                    }
                } else if (snapshotSplits.isEmpty() && snapshotSplitReader.canAssignNextSplit()) {
                    break;
                } // else continue to fetch records

                Thread.sleep(300);
                retry++;
            }
        } finally {
            snapshotSplitReader.close();
        }

        assertEquals(splitContext.getDocumentCount(), actualCount);
    }
}
