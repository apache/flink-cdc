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

package org.apache.flink.cdc.connectors.mongodb.source.reader;

import org.apache.flink.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SampleBucketSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.ShardedSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SingleSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SplitContext;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SplitVectorSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;

import static java.util.Collections.singletonList;
import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;

/** MongoDB snapshot split reader test case. */
class MongoDBSnapshotSplitReaderTest extends MongoDBSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotSplitReaderTest.class);

    private static final int MAX_RETRY_TIMES = 100;

    private String database;

    private MongoDBSourceConfig sourceConfig;

    private MongoDBDialect dialect;

    private SplitContext splitContext;

    @BeforeEach
    public void before() {
        database = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("chunk_test");

        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts(MONGO_CONTAINER.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + ".shopping_cart")
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .splitSizeMB(1)
                        .samplesPerChunk(10)
                        .pollAwaitTimeMillis(500);

        sourceConfig = configFactory.create(0);

        dialect = new MongoDBDialect();

        splitContext = SplitContext.of(sourceConfig, new TableId(database, null, "shopping_cart"));
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithShardedSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(ShardedSplitStrategy.INSTANCE);
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithSplitVectorSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(SplitVectorSplitStrategy.INSTANCE);
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithSamplerSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(SampleBucketSplitStrategy.INSTANCE);
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithSingleSplitter() throws Exception {
        testMongoDBSnapshotSplitReader(SingleSplitStrategy.INSTANCE);
    }

    private void testMongoDBSnapshotSplitReader(SplitStrategy splitter) throws Exception {
        LinkedList<SnapshotSplit> snapshotSplits = new LinkedList<>(splitter.split(splitContext));
        Assertions.assertThat(snapshotSplits).isNotEmpty();

        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(new TestingReaderContext());
        IncrementalSourceSplitReader<MongoDBSourceConfig> snapshotSplitReader =
                new IncrementalSourceSplitReader<>(
                        0,
                        dialect,
                        sourceConfig,
                        incrementalSourceReaderContext,
                        SnapshotPhaseHooks.empty());

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

                                Assertions.assertThat(productKind).isEqualTo("KIND_" + productNo);
                                Assertions.assertThat(userId).isEqualTo("user_" + productNo);
                                Assertions.assertThat(description)
                                        .isEqualTo("my shopping cart " + productNo);
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

        Assertions.assertThat(actualCount).isEqualTo(splitContext.getDocumentCount());
    }
}
