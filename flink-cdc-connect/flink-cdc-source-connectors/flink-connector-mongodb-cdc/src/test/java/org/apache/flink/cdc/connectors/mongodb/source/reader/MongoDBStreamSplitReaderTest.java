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
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import org.apache.flink.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.flink.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import org.apache.flink.cdc.connectors.mongodb.source.offset.ChangeStreamOffsetFactory;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.OperationType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.OPERATION_TYPE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionNames;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionsFilter;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseFilter;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseNames;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamDescriptor;
import static org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils.getLatestResumeToken;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;

/** MongoDB stream split reader test case. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class MongoDBStreamSplitReaderTest extends MongoDBSourceTestBase {

    private static final String STREAM_SPLIT_ID = "stream-split";

    private static final int MAX_RETRY_TIMES = 100;

    private String database;

    private MongoDBDialect dialect;

    private MongoDBSourceConfig sourceConfig;

    private ChangeStreamOffsetFactory changeStreamOffsetFactory;

    private ChangeStreamDescriptor changeStreamDescriptor;

    private BsonDocument startupResumeToken;

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
                        .pollAwaitTimeMillis(500);

        dialect = new MongoDBDialect();

        sourceConfig = configFactory.create(0);

        changeStreamOffsetFactory = new ChangeStreamOffsetFactory();

        List<String> discoveredDatabases =
                databaseNames(mongodbClient, databaseFilter(sourceConfig.getDatabaseList()));
        List<String> discoveredCollections =
                collectionNames(
                        mongodbClient,
                        discoveredDatabases,
                        collectionsFilter(sourceConfig.getCollectionList()));

        changeStreamDescriptor =
                getChangeStreamDescriptor(sourceConfig, discoveredDatabases, discoveredCollections);

        startupResumeToken = getLatestResumeToken(mongodbClient, changeStreamDescriptor);
    }

    @Test
    void testStreamSplitReader() throws Exception {
        IncrementalSourceReaderContext incrementalSourceReaderContext =
                new IncrementalSourceReaderContext(new TestingReaderContext());

        try (IncrementalSourceSplitReader<MongoDBSourceConfig> streamSplitReader =
                new IncrementalSourceSplitReader<>(
                        0,
                        dialect,
                        sourceConfig,
                        incrementalSourceReaderContext,
                        SnapshotPhaseHooks.empty())) {
            ChangeStreamOffset startOffset = new ChangeStreamOffset(startupResumeToken);

            StreamSplit streamSplit =
                    new StreamSplit(
                            STREAM_SPLIT_ID,
                            startOffset,
                            changeStreamOffsetFactory.createNoStoppingOffset(),
                            new ArrayList<>(),
                            new HashMap<>(),
                            0);

            Assertions.assertThat(streamSplitReader.canAssignNextSplit()).isTrue();
            streamSplitReader.handleSplitsChanges(new SplitsAddition<>(singletonList(streamSplit)));

            MongoCollection<Document> collection =
                    mongodbClient.getDatabase(database).getCollection("shopping_cart");

            long now = System.currentTimeMillis();
            List<Document> inserts =
                    Arrays.asList(
                            shoppingCartDoc(now),
                            shoppingCartDoc(now + 1),
                            shoppingCartDoc(now + 2),
                            shoppingCartDoc(now + 3));
            collection.insertMany(inserts);

            int retry = 0;
            int count = 0;
            while (retry < MAX_RETRY_TIMES) {
                ChangeEventRecords records = (ChangeEventRecords) streamSplitReader.fetch();
                if (records.nextSplit() != null) {
                    SourceRecords sourceRecords;
                    while ((sourceRecords = records.nextRecordFromSplit()) != null) {
                        Iterator<SourceRecord> iterator = sourceRecords.iterator();
                        while (iterator.hasNext()) {
                            Struct value = (Struct) iterator.next().value();
                            OperationType operationType =
                                    OperationType.fromString(value.getString(OPERATION_TYPE_FIELD));

                            Assertions.assertThat(operationType).isEqualTo(OperationType.INSERT);
                            BsonDocument fullDocument =
                                    BsonDocument.parse(value.getString(FULL_DOCUMENT_FIELD));
                            long productNo = fullDocument.getInt64("product_no").longValue();
                            String productKind = fullDocument.getString("product_kind").getValue();
                            String userId = fullDocument.getString("user_id").getValue();
                            String description = fullDocument.getString("description").getValue();

                            Assertions.assertThat(productKind).isEqualTo("KIND_" + productNo);
                            Assertions.assertThat(userId).isEqualTo("user_" + productNo);
                            Assertions.assertThat(description)
                                    .isEqualTo("my shopping cart " + productNo);

                            if (++count >= inserts.size()) {
                                return;
                            }
                        }
                    }
                } else {
                    break;
                }
                Thread.sleep(300);
                retry++;
            }

            Assertions.assertThat(inserts).hasSize(count);
        }
    }

    private Document shoppingCartDoc(long productNo) {
        Document document = new Document();
        document.put("product_no", productNo);
        document.put("product_kind", "KIND_" + productNo);
        document.put("user_id", "user_" + productNo);
        document.put("description", "my shopping cart " + productNo);
        return document;
    }
}
