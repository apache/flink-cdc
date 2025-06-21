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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBAssertUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBTestUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.util.Preconditions.checkState;

/** IT tests to cover various newly added collections during capture process. */
@Timeout(value = 500, unit = TimeUnit.SECONDS)
class NewlyAddedTableITCase extends MongoDBSourceTestBase {

    private String customerDatabase;
    protected static final int DEFAULT_PARALLELISM = 4;

    @TempDir private static Path tempDir;

    private final ScheduledExecutorService mockChangelogExecutor =
            Executors.newScheduledThreadPool(1);

    @BeforeEach
    public void before() throws SQLException {
        customerDatabase = "customer_" + Integer.toUnsignedString(new Random().nextInt(), 36);
        TestValuesTableFactory.clearAllData();
        // prepare initial data for given collection
        String collectionName = "produce_changelog";
        // enable system-level fulldoc pre & post image feature
        MONGO_CONTAINER.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");

        // mock continuous changelog during the newly added collections capturing process
        MongoDatabase mongoDatabase = mongodbClient.getDatabase(customerDatabase);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        mockChangelogExecutor.schedule(
                () -> {
                    Document document = new Document();
                    document.put("cid", 1);
                    document.put("cnt", 103L);
                    mongoCollection.insertOne(document);
                    mongoCollection.deleteOne(Filters.eq("cid", 1));
                },
                500,
                TimeUnit.MICROSECONDS);
    }

    @AfterEach
    public void after() {
        mockChangelogExecutor.shutdown();
        if (mongodbClient != null) {
            MongoDatabase mongoDatabase = mongodbClient.getDatabase(customerDatabase);
            mongoDatabase.drop();
        }
        miniClusterResource.get().cancelAllJobs();
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineOnce() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineOnceWithAheadOplog() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineTwice() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineTwiceWithAheadOplog() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineTwiceWithAheadOplogAndAutoCloseReader()
            throws Exception {
        Map<String, String> otherOptions = new HashMap<>();
        otherOptions.put("scan.incremental.close-idle-reader.enabled", "true");
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                otherOptions,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineThrice() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineThriceWithAheadOplog() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testNewlyAddedCollectionForExistsPipelineSingleParallelismWithAheadOplog()
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedCollection() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForNewlyAddedCollectionWithAheadOplog() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedCollection() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testTaskManagerFailoverForNewlyAddedCollectionWithAheadOplog() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    void testJobManagerFailoverForRemoveCollectionSingleParallelism() throws Exception {
        testRemoveCollectionsOneByOne(
                1,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testJobManagerFailoverForRemoveCollection() throws Exception {
        testRemoveCollectionsOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testTaskManagerFailoverForRemoveCollectionSingleParallelism() throws Exception {
        testRemoveCollectionsOneByOne(
                1,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testTaskManagerFailoverForRemoveCollection() throws Exception {
        testRemoveCollectionsOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveCollectionSingleParallelism() throws Exception {
        testRemoveCollectionsOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveCollection() throws Exception {
        testRemoveCollectionsOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    void testRemoveAndAddCollectionsOneByOne() throws Exception {
        testRemoveAndAddCollectionsOneByOne(
                1, "address_hangzhou", "address_beijing", "address_shanghai", "address_shenzhen");
    }

    private void testRemoveAndAddCollectionsOneByOne(
            int parallelism, String... captureAddressCollections) throws Exception {

        MongoDatabase database = mongodbClient.getDatabase(customerDatabase);
        // step 1: create mongodb collections with all collections included
        initialAddressCollections(database, captureAddressCollections);

        final String savepointDirectory = tempDir.toString();

        // get all expected data
        List<String> fetchedDataList = new ArrayList<>();

        String finishedSavePointPath = null;
        // step 2: execute insert and trigger savepoint with first collections added
        {
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String collection0 = captureAddressCollections[0];
            String cityName0 = collection0.split("_")[1];
            String createTableStatement = getCreateTableStatement(new HashMap<>(), collection0);

            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " cid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (collection_name,cid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, cid, country, city, detail_address from address");
            JobClient jobClient = tableResult.getJobClient().get();
            // first round's snapshot data
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    collection0, cityName0, cityName0),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    collection0, cityName0, cityName0),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    collection0, cityName0, cityName0)));

            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // first round's changelog data
            makeOplogForAddressTableInRound(database, collection0, 0);
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "-U[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    collection0, cityName0, cityName0),
                            format(
                                    "+U[%s, 416874195632735147, China_0, %s, %s West Town address 1]",
                                    collection0, cityName0, cityName0),
                            format(
                                    "+I[%s, %d, China, %s, %s West Town address 4]",
                                    collection0, 417022095255614380L, cityName0, cityName0)));
            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }

        // step 3: test adding and removing collections one by one
        for (int round = 1; round < captureAddressCollections.length; round++) {
            String captureTableThisRound = captureAddressCollections[round];
            String cityName = captureTableThisRound.split("_")[1];
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(
                            new HashMap<>(), captureAddressCollections[0], captureTableThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " cid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (collection_name,cid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, cid, country, city, detail_address from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // this round's snapshot data
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    captureTableThisRound, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    captureTableThisRound, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    captureTableThisRound, cityName, cityName)));
            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 4: make changelog data for all collections before this round(also includes this
            // round),
            // test whether only this round collection's data is captured.
            for (int i = 0; i <= round; i++) {
                String collection = captureAddressCollections[i];
                makeOplogForAddressTableInRound(database, collection, round);
            }
            // this round's changelog data
            String collection0 = captureAddressCollections[0];
            String cityName0 = collection0.split("_")[1];
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "-U[%s, 416874195632735147, China_%s, %s, %s West Town address 1]",
                                    collection0, round - 1, cityName0, cityName0),
                            format(
                                    "+U[%s, 416874195632735147, China_%s, %s, %s West Town address 1]",
                                    collection0, round, cityName0, cityName0),
                            format(
                                    "+I[%s, %d, China, %s, %s West Town address 4]",
                                    collection0,
                                    417022095255614380L + round,
                                    cityName0,
                                    cityName0)));

            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "-U[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    captureTableThisRound, cityName, cityName),
                            format(
                                    "+U[%s, 416874195632735147, China_%s, %s, %s West Town address 1]",
                                    captureTableThisRound, round, cityName, cityName),
                            format(
                                    "+I[%s, %d, China, %s, %s West Town address 4]",
                                    captureTableThisRound,
                                    417022095255614380L + round,
                                    cityName,
                                    cityName)));

            // assert fetched changelog data in this round
            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());

            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));
            // step 6: trigger savepoint
            if (round != captureAddressCollections.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private void testRemoveCollectionsOneByOne(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String... captureAddressCollections)
            throws Exception {

        // step 1: create mongdb collections with all collections included
        initialAddressCollections(
                mongodbClient.getDatabase(customerDatabase), captureAddressCollections);

        final String savepointDirectory = tempDir.toString();

        // get all expected data
        List<String> fetchedDataList = new ArrayList<>();
        for (String collection : captureAddressCollections) {
            String cityName = collection.split("_")[1];
            fetchedDataList.addAll(
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    collection, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    collection, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    collection, cityName, cityName)));
        }

        String finishedSavePointPath = null;
        // step 2: execute insert and trigger savepoint with all collections added
        {
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureAddressCollections);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " cid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (collection_name,cid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, cid, country, city, detail_address from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // trigger failover after some snapshot data read finished
            if (failoverPhase == MongoDBTestUtils.FailoverPhase.SNAPSHOT) {
                MongoDBTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));
            // sleep 1s to wait for the assign status to INITIAL_ASSIGNING_FINISHED.
            // Otherwise, the restart job won't read newly added tables, and this test will be
            // stuck.
            Thread.sleep(1000L);
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }

        // test removing collection one by one, note that there should be at least one collection
        // remaining
        for (int round = 0; round < captureAddressCollections.length - 1; round++) {
            String[] captureTablesThisRound =
                    Arrays.asList(captureAddressCollections)
                            .subList(round + 1, captureAddressCollections.length)
                            .toArray(new String[0]);

            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(new HashMap<>(), captureTablesThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " cid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (collection_name,cid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, cid, country, city, detail_address from address");
            JobClient jobClient = tableResult.getJobClient().get();

            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 3: make oplog data for all collections
            List<String> expectedOplogDataThisRound = new ArrayList<>();

            for (int i = 0, captureAddressTablesLength = captureAddressCollections.length;
                    i < captureAddressTablesLength;
                    i++) {
                String collectionName = captureAddressCollections[i];
                makeOplogForAddressTableInRound(
                        mongodbClient.getDatabase(customerDatabase), collectionName, round);

                if (i <= round) {
                    continue;
                }
                String cityName = collectionName.split("_")[1];
                expectedOplogDataThisRound.addAll(
                        Arrays.asList(
                                format(
                                        "-U[%s, 416874195632735147, China%s, %s, %s West Town address 1]",
                                        collectionName,
                                        round == 0 ? "" : "_" + (round - 1),
                                        cityName,
                                        cityName),
                                format(
                                        "+U[%s, 416874195632735147, China_%s, %s, %s West Town address 1]",
                                        collectionName, round, cityName, cityName),
                                format(
                                        "+I[%s, %d, China, %s, %s West Town address 4]",
                                        collectionName,
                                        417022095255614380L + round,
                                        cityName,
                                        cityName)));
            }

            if (failoverPhase == MongoDBTestUtils.FailoverPhase.STREAM
                    && TestValuesTableFactory.getRawResultsAsStrings("sink").size()
                            > fetchedDataList.size()) {
                MongoDBTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }

            fetchedDataList.addAll(expectedOplogDataThisRound);
            // step 4: assert fetched oplog data in this round
            MongoDBTestUtils.waitForSinkSize("sink", fetchedDataList.size());

            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getRawResultsAsStrings("sink"));

            // step 5: trigger savepoint
            finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            jobClient.cancel().get();
        }
    }

    private void testNewlyAddedCollectionOneByOne(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            boolean makeOplogBeforeCapture,
            String... captureAddressCollections)
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                parallelism,
                new HashMap<>(),
                failoverType,
                failoverPhase,
                makeOplogBeforeCapture,
                captureAddressCollections);
    }

    private void testNewlyAddedCollectionOneByOne(
            int parallelism,
            Map<String, String> sourceOptions,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            boolean makeOplogBeforeCapture,
            String... captureAddressCollections)
            throws Exception {

        // step 1: create mongodb collections with initial data
        initialAddressCollections(
                mongodbClient.getDatabase(customerDatabase), captureAddressCollections);

        final String savepointDirectory = tempDir.toString();

        // test newly added collection one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressCollections.length; round++) {
            String[] captureCollectionsThisRound =
                    Arrays.copyOf(captureAddressCollections, round + 1);
            String newlyAddedCollection = captureAddressCollections[round];
            if (makeOplogBeforeCapture) {
                makeOplogBeforeCaptureForAddressCollection(
                        mongodbClient.getDatabase(customerDatabase), newlyAddedCollection);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(sourceOptions, captureCollectionsThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " cid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (collection_name,cid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, cid, country, city, detail_address from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // step 2: assert fetched snapshot data in this round
            String cityName = newlyAddedCollection.split("_")[1];
            List<String> expectedSnapshotDataThisRound =
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    newlyAddedCollection, cityName, cityName));
            if (makeOplogBeforeCapture) {
                expectedSnapshotDataThisRound =
                        Arrays.asList(
                                format(
                                        "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                        newlyAddedCollection, cityName, cityName),
                                format(
                                        "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                        newlyAddedCollection, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                        newlyAddedCollection, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614381, China, %s, %s West Town address 5]",
                                        newlyAddedCollection, cityName, cityName));
            }

            // trigger failover after some snapshot data read finished
            if (failoverPhase == MongoDBTestUtils.FailoverPhase.SNAPSHOT) {
                MongoDBTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));
            // Wait 1s until snapshot phase finished, make sure the binlog data is not lost.
            Thread.sleep(1000L);

            // step 3: make some changelog data for this round
            makeFirstPartOplogForAddressCollection(
                    mongodbClient.getDatabase(customerDatabase), newlyAddedCollection);
            if (failoverPhase == MongoDBTestUtils.FailoverPhase.STREAM) {
                MongoDBTestUtils.triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.get().getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartOplogForAddressCollections(
                    mongodbClient.getDatabase(customerDatabase), newlyAddedCollection);

            // step 4: assert fetched changelog data in this round

            // retract the old data with id 416874195632735147
            fetchedDataList =
                    fetchedDataList.stream()
                            .filter(
                                    r ->
                                            !r.contains(
                                                    format(
                                                            "%s, 416874195632735147",
                                                            newlyAddedCollection)))
                            .collect(Collectors.toList());
            List<String> expectedOplogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedCollection, cityName, cityName));

            // step 5: assert fetched changelog data in this round
            fetchedDataList.addAll(expectedOplogUpsertDataThisRound);

            waitForUpsertSinkSize("sink", fetchedDataList.size());
            // the result size of sink may arrive fetchedDataList.size() with old data, wait one
            // checkpoint to wait retract old record and send new record
            Thread.sleep(1000L);
            MongoDBAssertUtils.assertEqualsInAnyOrder(
                    fetchedDataList, TestValuesTableFactory.getResultsAsStrings("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressCollections.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private void initialAddressCollections(
            MongoDatabase mongoDatabase, String[] captureCustomerCollections) {
        for (String collectionName : captureCustomerCollections) {
            // make initial data for given collection.
            String cityName = collectionName.split("_")[1];
            // B - enable collection-level fulldoc pre & post image for change capture collection
            MONGO_CONTAINER.executeCommandInDatabase(
                    String.format(
                            "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                            collectionName, collectionName),
                    mongoDatabase.getName());
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            collection.insertOne(
                    addressDocOf(
                            416874195632735147L,
                            "China",
                            cityName,
                            cityName + " West Town address 1"));
            collection.insertOne(
                    addressDocOf(
                            416927583791428523L,
                            "China",
                            cityName,
                            cityName + " West Town address 2"));
            collection.insertOne(
                    addressDocOf(
                            417022095255614379L,
                            "China",
                            cityName,
                            cityName + " West Town address 3"));
        }
    }

    private void makeFirstPartOplogForAddressCollection(
            MongoDatabase mongoDatabase, String collectionName) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.updateOne(
                Filters.eq("cid", 416874195632735147L), Updates.set("country", "CHINA"));
    }

    private void makeSecondPartOplogForAddressCollections(
            MongoDatabase mongoDatabase, String collectionName) {
        String cityName = collectionName.split("_")[1];
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.insertOne(
                addressDocOf(
                        417022095255614380L, "China", cityName, cityName + " West Town address 4"));
    }

    private void makeOplogBeforeCaptureForAddressCollection(
            MongoDatabase mongoDatabase, String collectionName) {
        String cityName = collectionName.split("_")[1];
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.insertOne(
                addressDocOf(
                        417022095255614381L, "China", cityName, cityName + " West Town address 5"));
    }

    private void makeOplogForAddressTableInRound(
            MongoDatabase mongoDatabase, String collectionName, int round) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        // make oplog events for the first split
        String cityName = collectionName.split("_")[1];
        collection.updateOne(
                Filters.eq("cid", 416874195632735147L), Updates.set("country", "China_" + round));
        collection.insertOne(
                addressDocOf(
                        417022095255614380L + round,
                        "China",
                        cityName,
                        cityName + " West Town address 4"));
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient.triggerSavepoint(savepointDirectory).get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironmentFromSavePoint(
            String finishedSavePointPath, int parallelism) throws Exception {
        // Close sink upsert materialize to show more clear test output.
        Configuration tableConfig = new Configuration();
        tableConfig.setString("table.exec.sink.upsert-materialize", "none");
        if (finishedSavePointPath != null) {
            tableConfig.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(tableConfig);
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
    }

    private String getCreateTableStatement(
            Map<String, String> otherOptions, String... captureTableNames) {
        return String.format(
                "CREATE TABLE address ("
                        + " _id STRING NOT NULL,"
                        + " collection_name STRING METADATA VIRTUAL,"
                        + " cid BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (_id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mongodb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hosts' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database' = '%s',"
                        + " 'collection' = '%s',"
                        + " 'chunk-meta.group.size' = '2',"
                        + " 'heartbeat.interval.ms' = '100',"
                        + " 'scan.full-changelog' = 'true',"
                        + " 'scan.newly-added-table.enabled' = 'true'"
                        + " %s"
                        + ")",
                MONGO_CONTAINER.getHostAndPort(),
                FLINK_USER,
                FLINK_USER_PASSWORD,
                customerDatabase,
                getCollectionNameRegex(customerDatabase, captureTableNames),
                otherOptions.isEmpty()
                        ? ""
                        : ","
                                + otherOptions.entrySet().stream()
                                        .map(
                                                e ->
                                                        String.format(
                                                                "'%s'='%s'",
                                                                e.getKey(), e.getValue()))
                                        .collect(Collectors.joining(",")));
    }

    protected static void waitForUpsertSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (upsertSinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    protected static int upsertSinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getResultsAsStrings(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private String getCollectionNameRegex(String database, String[] captureCustomerCollections) {
        checkState(captureCustomerCollections.length > 0);
        if (captureCustomerCollections.length == 1) {
            return captureCustomerCollections[0];
        } else {
            // pattern that matches multiple collections
            return Arrays.stream(captureCustomerCollections)
                    .map(coll -> "^(" + database + "." + coll + ")$")
                    .collect(Collectors.joining("|"));
        }
    }

    private Document addressDocOf(Long cid, String country, String city, String detailAddress) {
        Document document = new Document();
        document.put("cid", cid);
        document.put("country", country);
        document.put("city", city);
        document.put("detail_address", detailAddress);
        return document;
    }
}
