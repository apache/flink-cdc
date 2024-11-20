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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import com.mongodb.client.MongoDatabase;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBTestUtils.waitForSinkSize;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBTestUtils.waitForSnapshotStarted;

/** Integration tests to check mongodb-cdc works well under namespace.regex. */
class MongoDBRegexFilterITCase extends MongoDBSourceTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    void setup(boolean parallelismSnapshot) {
        TestValuesTableFactory.clearAllData();
        if (parallelismSnapshot) {
            env.setParallelism(DEFAULT_PARALLELISM);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    /** match multiple databases and collections: collection = ^(db0|db1)\.coll_a\d?$ . */
    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMatchMultipleDatabasesAndCollections(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // 1. Given collections:
        // db0: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db0 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");
        // db1: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db1 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");

        // 2. Test match: collection = ^(db0|db1)\.coll_a\d?$
        String collectionRegex = String.format("^(%s|%s)\\.coll_a\\d?$", db0, db1);
        TableResult result = submitTestCase(null, collectionRegex, parallelismSnapshot);

        // 3. Wait snapshot finished
        waitForSinkSize("mongodb_sink", 4);

        // 4. Insert new records in database: [coll_a1.A102, coll_a2.A202, coll_b1.B102,
        // coll_b1.B102]
        insertRecordsInDatabase(db0);
        insertRecordsInDatabase(db1);

        // 5. Wait change stream records come
        waitForSinkSize("mongodb_sink", 8);

        // 6. Check results
        String[] expected =
                new String[] {
                    String.format("+I[%s, coll_a1, A101]", db0),
                    String.format("+I[%s, coll_a2, A201]", db0),
                    String.format("+I[%s, coll_a1, A101]", db1),
                    String.format("+I[%s, coll_a2, A201]", db1),
                    String.format("+I[%s, coll_a1, A102]", db0),
                    String.format("+I[%s, coll_a2, A202]", db0),
                    String.format("+I[%s, coll_a1, A102]", db1),
                    String.format("+I[%s, coll_a2, A202]", db1)
                };

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("mongodb_sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    /** match multiple databases: database = db0|db1 . */
    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMatchMultipleDatabases(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // 1. Given collections:
        // db0: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db0 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");
        // db1: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db1 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");
        // db2: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db2 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");

        // 2. Test match database: ^(db0|db1)$
        String databaseRegex = String.format("%s|%s", db0, db1);
        TableResult result = submitTestCase(databaseRegex, null, parallelismSnapshot);

        // 3. Wait snapshot finished
        waitForSinkSize("mongodb_sink", 8);

        // 4. Insert new records in database: [coll_a1.A102, coll_a2.A202, coll_b1.B102,
        // coll_b1.B102]
        insertRecordsInDatabase(db0);
        insertRecordsInDatabase(db1);
        insertRecordsInDatabase(db2);

        // 5. Wait change stream records come
        waitForSinkSize("mongodb_sink", 16);

        // 6. Check results
        String[] expected =
                new String[] {
                    String.format("+I[%s, coll_a1, A101]", db0),
                    String.format("+I[%s, coll_a2, A201]", db0),
                    String.format("+I[%s, coll_b1, B101]", db0),
                    String.format("+I[%s, coll_b2, B201]", db0),
                    String.format("+I[%s, coll_a1, A101]", db1),
                    String.format("+I[%s, coll_a2, A201]", db1),
                    String.format("+I[%s, coll_b1, B101]", db1),
                    String.format("+I[%s, coll_b2, B201]", db1),
                    String.format("+I[%s, coll_a1, A102]", db0),
                    String.format("+I[%s, coll_a2, A202]", db0),
                    String.format("+I[%s, coll_b1, B102]", db0),
                    String.format("+I[%s, coll_b2, B202]", db0),
                    String.format("+I[%s, coll_a1, A102]", db1),
                    String.format("+I[%s, coll_a2, A202]", db1),
                    String.format("+I[%s, coll_b1, B102]", db1),
                    String.format("+I[%s, coll_b2, B202]", db1),
                };

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("mongodb_sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    /** match single database and multiple collections: collection = ^db0\.coll_b\d?$ . */
    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMatchSingleQualifiedCollectionPattern(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // 1. Given collections:
        // db0: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db0 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");
        // db1: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db1 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");

        // 2. Test match: collection ^(db0|db1)\.coll_a\d?$
        String collectionRegex = String.format("^%s\\.coll_b\\d?$", db0);
        TableResult result = submitTestCase(null, collectionRegex, parallelismSnapshot);

        // 3. Wait snapshot finished
        waitForSinkSize("mongodb_sink", 2);

        // 4. Insert new records in database: [coll_a1.A102, coll_a2.A202, coll_b1.B102,
        // coll_b1.B102]
        insertRecordsInDatabase(db0);
        insertRecordsInDatabase(db1);

        // 5. Wait change stream records come
        waitForSinkSize("mongodb_sink", 4);

        // 6. Check results
        String[] expected =
                new String[] {
                    String.format("+I[%s, coll_b1, B101]", db0),
                    String.format("+I[%s, coll_b2, B201]", db0),
                    String.format("+I[%s, coll_b1, B102]", db0),
                    String.format("+I[%s, coll_b2, B202]", db0)
                };

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("mongodb_sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    /** match single database and multiple collections: database = db0 collection = .*coll_b\d? . */
    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMatchSingleDatabaseWithCollectionPattern(boolean parallelismSnapshot)
            throws Exception {
        setup(parallelismSnapshot);
        // 1. Given collections:
        // db0: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db0 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");
        // db1: [coll_a1, coll_a2, coll_b1, coll_b2]
        String db1 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns_regex");

        // 2. Test match: collection .*coll_b\d?
        String collectionRegex = ".*coll_b\\d?";
        TableResult result = submitTestCase(db0, collectionRegex, parallelismSnapshot);

        // 3. Wait snapshot finished
        waitForSinkSize("mongodb_sink", 2);

        // 4. Insert new records in database: [coll_a1.A102, coll_a2.A202, coll_b1.B102,
        // coll_b1.B102]
        insertRecordsInDatabase(db0);
        insertRecordsInDatabase(db1);

        // 5. Wait change stream records come
        waitForSinkSize("mongodb_sink", 4);

        // 6. Check results
        String[] expected =
                new String[] {
                    String.format("+I[%s, coll_b1, B101]", db0),
                    String.format("+I[%s, coll_b2, B201]", db0),
                    String.format("+I[%s, coll_b1, B102]", db0),
                    String.format("+I[%s, coll_b2, B202]", db0)
                };

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("mongodb_sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMatchDatabaseAndCollectionContainsDash(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // 1. Given collections:
        // db0: [coll-a1, coll-a2, coll-b1, coll-b2]
        String db0 = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns-regex");

        TableResult result = submitTestCase(db0, "coll-a1", parallelismSnapshot);

        // 2. Wait change stream records come
        waitForSinkSize("mongodb_sink", 1);

        // 3. Check results
        String[] expected = new String[] {String.format("+I[%s, coll-a1, A101]", db0)};

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("mongodb_sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testMatchCollectionWithDots(boolean parallelismSnapshot) throws Exception {
        setup(parallelismSnapshot);
        // 1. Given colllections:
        // db: [coll.name]
        String db = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("ns-dotted");

        TableResult result = submitTestCase(db, db + "[.]coll[.]name", parallelismSnapshot);

        // 2. Wait change stream records come
        waitForSinkSize("mongodb_sink", 3);

        // 3. Check results
        String[] expected =
                new String[] {
                    String.format("+I[%s, coll.name, A101]", db),
                    String.format("+I[%s, coll.name, A102]", db),
                    String.format("+I[%s, coll.name, A103]", db)
                };

        List<String> actual = TestValuesTableFactory.getRawResultsAsStrings("mongodb_sink");
        Assertions.assertThat(actual).containsExactlyInAnyOrder(expected);

        result.getJobClient().get().cancel().get();
    }

    private TableResult submitTestCase(
            String database, String collection, boolean parallelismSnapshot) throws Exception {
        String sourceDDL =
                "CREATE TABLE mongodb_source ("
                        + " _id STRING NOT NULL,"
                        + " seq STRING,"
                        + " db_name STRING METADATA FROM 'database_name' VIRTUAL,"
                        + " coll_name STRING METADATA FROM 'collection_name' VIRTUAL,"
                        + " PRIMARY KEY (_id) NOT ENFORCED"
                        + ") WITH ("
                        + ignoreIfNull("hosts", MONGO_CONTAINER.getHostAndPort())
                        + ignoreIfNull("username", FLINK_USER)
                        + ignoreIfNull("password", FLINK_USER_PASSWORD)
                        + ignoreIfNull("database", database)
                        + ignoreIfNull("collection", collection)
                        + " 'scan.incremental.snapshot.enabled' = '"
                        + parallelismSnapshot
                        + "',"
                        + " 'connector' = 'mongodb-cdc'"
                        + ")";

        String sinkDDL =
                "CREATE TABLE mongodb_sink ("
                        + " db_name STRING,"
                        + " coll_name STRING,"
                        + " seq STRING,"
                        + " PRIMARY KEY (db_name, coll_name, seq) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO mongodb_sink SELECT db_name, coll_name, seq FROM mongodb_source");

        waitForSnapshotStarted("mongodb_sink");

        return result;
    }

    private String ignoreIfNull(String configName, String configValue) {
        return configValue != null ? String.format(" '%s' = '%s',", configName, configValue) : "";
    }

    private void insertRecordsInDatabase(String database) {
        MongoDatabase db = mongodbClient.getDatabase(database);
        db.getCollection("coll_a1").insertOne(new Document("seq", "A102"));
        db.getCollection("coll_a2").insertOne(new Document("seq", "A202"));
        db.getCollection("coll_b1").insertOne(new Document("seq", "B102"));
        db.getCollection("coll_b2").insertOne(new Document("seq", "B202"));
    }
}
