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

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.FailoverPhase;
import com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.FailoverType;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertEqualsInAnyOrder;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.fetchRows;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.triggerFailover;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.waitForSinkSize;
import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;

/** IT tests for {@link MongoDBSource}. */
public class MongoDBParallelSourceITCase extends MongoDBSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    @Test
    public void testReadSingleCollectionWithSingleParallelism() throws Exception {
        testMongoDBParallelSource(
                1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadSingleCollectionWithMultipleParallelism() throws Exception {
        testMongoDBParallelSource(
                4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"customers"});
    }

    @Test
    public void testReadMultipleCollectionWithSingleParallelism() throws Exception {
        testMongoDBParallelSource(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    @Test
    public void testReadMultipleCollectionWithMultipleParallelism() throws Exception {
        testMongoDBParallelSource(
                4,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"});
    }

    // Failover tests
    @Test
    public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testMongoDBParallelSource(
                FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverInStreamPhase() throws Exception {
        testMongoDBParallelSource(
                FailoverType.TM, FailoverPhase.STREAM, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testMongoDBParallelSource(
                FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testJobManagerFailoverInStreamPhase() throws Exception {
        testMongoDBParallelSource(
                FailoverType.JM, FailoverPhase.STREAM, new String[] {"customers", "customers_1"});
    }

    @Test
    public void testTaskManagerFailoverSingleParallelism() throws Exception {
        testMongoDBParallelSource(
                1, FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testJobManagerFailoverSingleParallelism() throws Exception {
        testMongoDBParallelSource(
                1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"customers"});
    }

    @Test
    public void testNewlyAddedCollectionForExistsPipelineOnce() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedCollectionForExistsPipelineOnceWithAheadChangeStream()
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedCollectionForExistsPipelineTwice() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedCollectionForExistsPipelineTwiceWithAheadChangeStream()
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedCollectionForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedCollectionForExistsPipelineSingleParallelismWithAheadChangeStream()
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                FailoverType.NONE,
                FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedCollection() throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedCollectionWithAheadChangeStream()
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                DEFAULT_PARALLELISM,
                FailoverType.JM,
                FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedCollection() throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedCollectionWithAheadChangeStream()
            throws Exception {
        testNewlyAddedCollectionOneByOne(
                1,
                FailoverType.TM,
                FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    private void testMongoDBParallelSource(
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerCollections)
            throws Exception {
        testMongoDBParallelSource(
                DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerCollections);
    }

    private void testMongoDBParallelSource(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            String[] captureCustomerCollections)
            throws Exception {

        String customerDatabase = ROUTER.executeCommandFileInSeparateDatabase("customer");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " _id STRING NOT NULL,"
                                + " cid BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'scan.parallelism.snapshot.enabled' = 'true',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s',"
                                + " 'heartbeat.interval.ms' = '500'"
                                + ")",
                        ROUTER.getHostAndPort(),
                        FLINK_USER,
                        FLINK_USER_PASSWORD,
                        customerDatabase,
                        getCollectionNameRegex(customerDatabase, captureCustomerCollections));
        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult =
                tEnv.executeSql("select cid, name, address, phone_number from customers");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerCollections.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        assertEqualsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the change stream data
        for (String collectionName : captureCustomerCollections) {
            makeFirstPartChangeStreamEvents(
                    mongodbClient.getDatabase(customerDatabase), collectionName);
        }
        if (failoverPhase == FailoverPhase.STREAM) {
            triggerFailover(
                    failoverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(200));
        }
        for (String collectionName : captureCustomerCollections) {
            makeSecondPartChangeStreamEvents(
                    mongodbClient.getDatabase(customerDatabase), collectionName);
        }

        String[] changeEventsForSingleTable =
                new String[] {
                    "-U[101, user_1, Shanghai, 123567891234]",
                    "+U[101, user_1, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> expectedChangeStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerCollections.length; i++) {
            expectedChangeStreamData.addAll(Arrays.asList(changeEventsForSingleTable));
        }
        assertEqualsInAnyOrder(
                expectedChangeStreamData, fetchRows(iterator, expectedChangeStreamData.size()));
        tableResult.getJobClient().get().cancel().get();
    }

    private void testNewlyAddedCollectionOneByOne(
            int parallelism,
            FailoverType failoverType,
            FailoverPhase failoverPhase,
            boolean makeChangeLogEventBeforeCapture,
            String... captureAddressCollections)
            throws Exception {

        TestValuesTableFactory.clearAllData();

        // step 1: create mongo collections with initial data
        String customerDatabase = ROUTER.executeCommandFileInSeparateDatabase("customer");

        initialAddressCollections(customerDatabase, captureAddressCollections);

        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final String savepointDirectory = temporaryFolder.newFolder().toURI().toString();

        // test newly added collection one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressCollections.length; round++) {
            String[] captureCollectionsThisRound =
                    Arrays.asList(captureAddressCollections)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedCollection = captureAddressCollections[round];
            if (makeChangeLogEventBeforeCapture) {
                makeChangeStreamEventBeforeCaptureForAddressCollection(
                        customerDatabase, newlyAddedCollection);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironment(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(customerDatabase, captureCollectionsThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " aid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (collection_name, aid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, aid, country, city, detail_address from address");
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
            if (makeChangeLogEventBeforeCapture) {
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
            if (failoverPhase == FailoverPhase.SNAPSHOT
                    && TestValuesTableFactory.getRawResults("sink").size()
                            > fetchedDataList.size()) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getRawResults("sink"));

            // step 3: make some change data for this round
            makeFirstPartChangeStreamEventForAddressCollection(
                    customerDatabase, newlyAddedCollection);
            if (failoverPhase == FailoverPhase.STREAM) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartChangeStreamEventForAddressCollection(
                    customerDatabase, newlyAddedCollection);

            // step 4: assert fetched change data in this round
            List<String> expectedChangeStreamEventsThisRound =
                    Arrays.asList(
                            format(
                                    "+U[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedCollection, cityName, cityName));

            // step 5: assert fetched change data in this round
            fetchedDataList.addAll(expectedChangeStreamEventsThisRound);
            waitForSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getRawResults("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressCollections.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment(
            String finishedSavePointPath, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (finishedSavePointPath != null) {
            // restore from savepoint
            // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz =
                    classLoader.loadClass(
                            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
            Method getConfigurationMethod = clazz.getDeclaredMethod("getConfiguration");
            getConfigurationMethod.setAccessible(true);
            Configuration configuration = (Configuration) getConfigurationMethod.invoke(env);
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
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

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private void makeFirstPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 101L), Updates.set("address", "Hangzhou"));
        mongoCollection.deleteOne(Filters.eq("cid", 102L));
        mongoCollection.insertOne(customerDocOf(102L, "user_2", "Shanghai", "123567891234"));
        mongoCollection.updateOne(Filters.eq("cid", 103L), Updates.set("address", "Hangzhou"));
    }

    private void makeSecondPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 1010L), Updates.set("address", "Hangzhou"));
        mongoCollection.insertMany(
                Arrays.asList(
                        customerDocOf(2001L, "user_22", "Shanghai", "123567891234"),
                        customerDocOf(2002L, "user_23", "Shanghai", "123567891234"),
                        customerDocOf(2003L, "user_24", "Shanghai", "123567891234")));
    }

    private Document customerDocOf(Long cid, String name, String address, String phoneNumber) {
        Document document = new Document();
        document.put("cid", cid);
        document.put("name", name);
        document.put("address", address);
        document.put("phone_number", phoneNumber);
        return document;
    }

    private void initialAddressCollections(String databaseName, String[] addressCollections) {
        for (String collectionName : addressCollections) {
            // make initial data for given table
            String cityName = collectionName.split("_")[1];

            MongoCollection<Document> collection =
                    mongodbClient.getDatabase(databaseName).getCollection(collectionName);

            collection.insertMany(
                    Arrays.asList(
                            addressDocOf(
                                    416874195632735147L,
                                    "China",
                                    cityName,
                                    cityName + " West Town address 1"),
                            addressDocOf(
                                    416927583791428523L,
                                    "China",
                                    cityName,
                                    cityName + " West Town address 2"),
                            addressDocOf(
                                    417022095255614379L,
                                    "China",
                                    cityName,
                                    cityName + " West Town address 3")));
        }
    }

    private void makeFirstPartChangeStreamEventForAddressCollection(
            String databaseName, String collectionName) {
        // make changeStream events for the first split
        MongoCollection<Document> collection =
                mongodbClient.getDatabase(databaseName).getCollection(collectionName);

        collection.updateOne(
                Filters.eq("aid", 416874195632735147L), Updates.set("country", "CHINA"));
    }

    private void makeSecondPartChangeStreamEventForAddressCollection(
            String databaseName, String collectionName) {
        String cityName = collectionName.split("_")[1];
        MongoCollection<Document> collection =
                mongodbClient.getDatabase(databaseName).getCollection(collectionName);

        collection.insertOne(
                addressDocOf(
                        417022095255614380L, "China", cityName, cityName + " West Town address 4"));
    }

    private void makeChangeStreamEventBeforeCaptureForAddressCollection(
            String databaseName, String collectionName) {
        String cityName = collectionName.split("_")[1];
        MongoCollection<Document> collection =
                mongodbClient.getDatabase(databaseName).getCollection(collectionName);

        collection.insertOne(
                addressDocOf(
                        417022095255614381L, "China", cityName, cityName + " West Town address 5"));
    }

    private String getCreateTableStatement(String databaseName, String... captureCollections) {
        return format(
                "CREATE TABLE address ("
                        + " collection_name STRING METADATA VIRTUAL,"
                        + " _id STRING NOT NULL,"
                        + " aid BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (_id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mongodb-cdc',"
                        + " 'scan.parallelism.snapshot.enabled' = 'true',"
                        + " 'scan.newly-added-collection.enabled' = 'true',"
                        + " 'hosts' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database' = '%s',"
                        + " 'collection' = '%s',"
                        + " 'heartbeat.interval.ms' = '500'"
                        + ")",
                ROUTER.getHostAndPort(),
                FLINK_USER,
                FLINK_USER_PASSWORD,
                databaseName,
                getCollectionNameRegex(databaseName, captureCollections));
    }

    private Document addressDocOf(Long aid, String country, String city, String detailAddress) {
        Document document = new Document();
        document.put("aid", aid);
        document.put("country", country);
        document.put("city", city);
        document.put("detail_address", detailAddress);
        return document;
    }
}
