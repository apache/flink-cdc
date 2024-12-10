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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mongodb.factory.MongoDBDataSourceFactory;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBSchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.util.CloseableIterator;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for MongoDB change stream event source. */
@RunWith(Parameterized.class)
public class MongoDBPipelineITCase extends MongoDBSourceTestBase {
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());
    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    private final boolean parallelismSnapshot;

    public MongoDBPipelineITCase(String mongoVersion, boolean parallelismSnapshot) {
        super(mongoVersion);
        this.parallelismSnapshot = parallelismSnapshot;
    }

    @Parameterized.Parameters(name = "mongoVersion: {0} parallelismSnapshot: {1}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {"6.0.16", true},
            new Object[] {"6.0.16", false},
            new Object[] {"7.0.12", true},
            new Object[] {"7.0.12", false}
        };
    }

    @Before
    public void before() {
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        if (parallelismSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @Test
    public void testInitialStartupMode() throws Exception {
        String database = mongoContainer.executeCommandFileInSeparateDatabase("inventory");

        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts(mongoContainer.getHostAndPort())
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .databaseList(database) // set captured database, support regex
                        .collectionList(database + "\\.products");
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MongoDBDataSource(configFactory).getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                MongoDBDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        TableId tableId = TableId.tableId(database, "products");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        Thread.sleep(2000);

        List<Event> expectedBinlog = new ArrayList<>();
        try (MongoClient client = mongodbClient) {
            MongoDatabase db = client.getDatabase(database);
            RowType rowType = MongoDBSchemaUtils.getJsonSchemaRowType();
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            db.getCollection("products")
                    .insertMany(
                            Arrays.asList(
                                    productDocOf(
                                            "100000000000000000000110",
                                            "jacket",
                                            "water resistent white wind breaker",
                                            0.2),
                                    productDocOf(
                                            "100000000000000000000111",
                                            "scooter",
                                            "Big 2-wheel scooter",
                                            5.18)));
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        BinaryStringData.fromString("100000000000000000000110"),
                                        BinaryStringData.fromString(
                                                "{\"_id\": {\"$oid\": \"100000000000000000000110\"}, \"name\": \"jacket\", \"description\": \"water resistent white wind breaker\", \"weight\": 0.2}")
                                    })));
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        BinaryStringData.fromString("100000000000000000000111"),
                                        BinaryStringData.fromString(
                                                "{\"_id\": {\"$oid\": \"100000000000000000000111\"}, \"name\": \"scooter\", \"description\": \"Big 2-wheel scooter\", \"weight\": 5.18}")
                                    })));
        }
        // In this configuration, several subtasks might emit their corresponding CreateTableEvent
        // to downstream. Since it is not possible to predict how many CreateTableEvents should we
        // expect, we simply filter them out from expected sets, and assert there's at least one.
        List<Event> actual =
                fetchResultsExcept(
                        events, expectedSnapshot.size() + expectedBinlog.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
        assertThat(actual.subList(expectedSnapshot.size(), actual.size()))
                .isEqualTo(expectedBinlog);
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size, T sideEvent) {
        List<T> result = new ArrayList<>(size);
        List<T> sideResults = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (!event.equals(sideEvent)) {
                result.add(event);
                size--;
            } else {
                sideResults.add(sideEvent);
            }
        }
        // Also ensure we've received at least one or many side events.
        assertThat(sideResults).isNotEmpty();
        return result;
    }

    private Document productDocOf(String id, String name, String description, Double weight) {
        Document document = new Document();
        if (id != null) {
            document.put("_id", new ObjectId(id));
        }
        document.put("name", name);
        document.put("description", description);
        document.put("weight", weight);
        return document;
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(tableId, MongoDBSchemaUtils.getJsonSchema());
    }

    private List<Event> getSnapshotExpected(TableId tableId) {
        RowType rowType = MongoDBSchemaUtils.getJsonSchemaRowType();
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000101"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000101\"}, \"name\": \"scooter\", \"description\": \"Small 2-wheel scooter\", \"weight\": 3.14}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000102"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000102\"}, \"name\": \"car battery\", \"description\": \"12V car battery\", \"weight\": 8.1}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000103"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000103\"}, \"name\": \"12-pack drill bits\", \"description\": \"12-pack of drill bits with sizes ranging from #40 to #3\", \"weight\": 0.8}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000104"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000104\"}, \"name\": \"hammer\", \"description\": \"12oz carpenter''s hammer\", \"weight\": 0.75}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000105"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000105\"}, \"name\": \"hammer\", \"description\": \"12oz carpenter''s hammer\", \"weight\": 0.875}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000106"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000106\"}, \"name\": \"hammer\", \"description\": \"12oz carpenter''s hammer\", \"weight\": 1}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000107"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000107\"}, \"name\": \"rocks\", \"description\": \"box of assorted rocks\", \"weight\": 5.3}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000108"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000108\"}, \"name\": \"jacket\", \"description\": \"water resistent black wind breaker\", \"weight\": 0.1}")
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("100000000000000000000109"),
                                    BinaryStringData.fromString(
                                            "{\"_id\": {\"$oid\": \"100000000000000000000109\"}, \"name\": \"spare tire\", \"description\": \"24 inch spare tire\", \"weight\": 22.2}")
                                })));
        return snapshotExpected;
    }
}
