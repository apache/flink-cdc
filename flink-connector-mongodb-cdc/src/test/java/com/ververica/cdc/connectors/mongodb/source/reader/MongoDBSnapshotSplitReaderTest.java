/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
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
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** MongoDB snapshot split reader test case. */
public class MongoDBSnapshotSplitReaderTest extends MongoDBSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotSplitReaderTest.class);

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
                        .chunkSizeMB(1)
                        .pollAwaitTimeMillis(500);

        sourceConfig = configFactory.create(0);

        dialect = new MongoDBDialect(configFactory);

        splitContext = SplitContext.of(sourceConfig, new CollectionId(database, "shopping_cart"));
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithShardedSplitter() throws IOException {
        testMongoDBSnapshotSplitReader(ShardedSplitStrategy.INSTANCE);
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithSplitVectorSplitter() throws IOException {
        testMongoDBSnapshotSplitReader(SplitVectorSplitStrategy.INSTANCE);
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithSamplerSplitter() throws IOException {
        testMongoDBSnapshotSplitReader(SampleBucketSplitStrategy.INSTANCE);
    }

    @Test
    public void testMongoDBSnapshotSplitReaderWithSingleSplitter() throws IOException {
        testMongoDBSnapshotSplitReader(SingleSplitStrategy.INSTANCE);
    }

    private void testMongoDBSnapshotSplitReader(SplitStrategy splitter) throws IOException {
        LinkedList<SnapshotSplit<CollectionId, CollectionSchema>> snapshotSplits =
                new LinkedList<>(splitter.split(splitContext));
        assertTrue(snapshotSplits.size() > 0);

        MongoDBSourceReaderContext sourceReaderContext =
                new MongoDBSourceReaderContext(new TestingReaderContext());
        MongoDBSourceSplitReader snapshotSplitReader =
                new MongoDBSourceSplitReader(dialect, sourceConfig, 0, sourceReaderContext);

        long actualCount = 0;
        try {
            while (true) {
                if (!snapshotSplits.isEmpty() && snapshotSplitReader.canAssignNextSplit()) {
                    SnapshotSplit<CollectionId, CollectionSchema> snapshotSplit =
                            snapshotSplits.poll();
                    LOG.info("Add snapshot split {}", snapshotSplit.splitId());
                    snapshotSplitReader.handleSplitsChanges(
                            new SplitsAddition<>(singletonList(snapshotSplit)));
                }

                ChangeEventRecords records = (ChangeEventRecords) snapshotSplitReader.fetch();
                if (records.nextSplit() != null) {
                    SourceRecord record;
                    while ((record = records.nextRecordFromSplit()) != null) {
                        Struct value = (Struct) record.value();
                        BsonDocument fullDocument =
                                BsonDocument.parse(value.getString(FULL_DOCUMENT_FIELD));
                        long productNo = fullDocument.getInt64("product_no").longValue();
                        String productKind = fullDocument.getString("product_kind").getValue();
                        String userId = fullDocument.getString("user_id").getValue();
                        String description = fullDocument.getString("description").getValue();

                        assertEquals("KIND_" + productNo, productKind);
                        assertEquals("user_" + productNo, userId);
                        assertEquals("my shopping cart " + productNo, description);
                        actualCount++;
                    }
                } else if (snapshotSplits.isEmpty() && snapshotSplitReader.canAssignNextSplit()) {
                    //                    snapshotSplitReader.close();
                    break;
                } // else continue to fetch records
            }
        } finally {
            snapshotSplitReader.close();
        }

        assertEquals(splitContext.getDocumentCount(), actualCount);
    }
}
