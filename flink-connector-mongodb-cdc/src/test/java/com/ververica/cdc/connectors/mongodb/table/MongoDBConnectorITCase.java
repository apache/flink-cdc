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

package com.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ververica.cdc.connectors.mongodb.MongoDBTestBase;
import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Integration tests for MongoDB change stream event SQL source. */
public class MongoDBConnectorITCase extends MongoDBTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    public void testConsumingAllEvents() throws ExecutionException, InterruptedException {
        String database = executeCommandFileInSeparateDatabase("inventory");

        String sourceDDL =
                String.format(
                        "CREATE TABLE mongodb_source ("
                                + " _id STRING NOT NULL,"
                                + " name STRING,"
                                + " description STRING,"
                                + " weight DECIMAL(10,3),"
                                + " PRIMARY KEY (_id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'connection.options' = 'connectTimeoutMS=12000&socketTimeoutMS=13000',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s'"
                                + ")",
                        MONGODB_CONTAINER.getHostAndPort(),
                        FLINK_USER,
                        FLINK_USER_PASSWORD,
                        database,
                        "products");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " name STRING,"
                        + " weightSum DECIMAL(10,3),"
                        + " PRIMARY KEY (name) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '20'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT name, SUM(weight) FROM mongodb_source GROUP BY name");

        waitForSnapshotStarted("sink");

        MongoCollection<Document> products = getMongoDatabase(database).getCollection("products");

        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000106")),
                Updates.set("description", "18oz carpenter hammer"));

        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000107")),
                Updates.set("weight", 5.1));

        products.insertOne(
                productDocOf(
                        "100000000000000000000110",
                        "jacket",
                        "water resistent white wind breaker",
                        0.2));

        products.insertOne(
                productDocOf("100000000000000000000111", "scooter", "Big 2-wheel scooter", 5.18));

        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000110")),
                Updates.combine(
                        Updates.set("description", "new water resistent white wind breaker"),
                        Updates.set("weight", 0.5)));

        products.updateOne(
                Filters.eq("_id", new ObjectId("100000000000000000000111")),
                Updates.set("weight", 5.17));

        products.deleteOne(Filters.eq("_id", new ObjectId("100000000000000000000111")));

        waitForSinkSize("sink", 20);

        // The final database table looks like this:
        //
        // > SELECT * FROM products;
        // +-----+--------------------+---------------------------------------------------------+--------+
        // | id  | name               | description                                             |
        // weight |
        // +-----+--------------------+---------------------------------------------------------+--------+
        // | 101 | scooter            | Small 2-wheel scooter                                   |
        // 3.14 |
        // | 102 | car battery        | 12V car battery                                         |
        // 8.1 |
        // | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |
        // 0.8 |
        // | 104 | hammer             | 12oz carpenter's hammer                                 |
        // 0.75 |
        // | 105 | hammer             | 14oz carpenter's hammer                                 |
        // 0.875 |
        // | 106 | hammer             | 18oz carpenter hammer                                   |
        //   1 |
        // | 107 | rocks              | box of assorted rocks                                   |
        // 5.1 |
        // | 108 | jacket             | water resistent black wind breaker                      |
        // 0.1 |
        // | 109 | spare tire         | 24 inch spare tire                                      |
        // 22.2 |
        // | 110 | jacket             | new water resistent white wind breaker                  |
        // 0.5 |
        // +-----+--------------------+---------------------------------------------------------+--------+

        String[] expected =
                new String[] {
                    "scooter,3.140",
                    "car battery,8.100",
                    "12-pack drill bits,0.800",
                    "hammer,2.625",
                    "rocks,5.100",
                    "jacket,0.600",
                    "spare tire,22.200"
                };

        List<String> actual = TestValuesTableFactory.getResults("sink");
        assertThat(actual, containsInAnyOrder(expected));

        result.getJobClient().get().cancel().get();
    }

    @Test
    public void testAllTypes() throws Throwable {
        executeCommandFile("column_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    _id STRING,\n"
                                + "    stringField STRING,\n"
                                + "    uuidField STRING,\n"
                                + "    md5Field STRING,\n"
                                + "    timeField TIME,\n"
                                + "    dateField DATE,\n"
                                + "    dateBefore1970 DATE,\n"
                                + "    dateToTimestampField TIMESTAMP(3),\n"
                                + "    dateToLocalTimestampField TIMESTAMP_LTZ(3),\n"
                                + "    timestampField TIMESTAMP(0),\n"
                                + "    timestampToLocalTimestampField TIMESTAMP_LTZ(0),\n"
                                + "    booleanField BOOLEAN,\n"
                                + "    decimal128Field DECIMAL ,\n"
                                + "    doubleField DOUBLE,\n"
                                + "    int32field INT,\n"
                                + "    int64Field BIGINT,\n"
                                + "    documentField ROW<a STRING,b BIGINT>,\n"
                                + "    mapField MAP<STRING,MAP<STRING,INT>>,\n"
                                + "    arrayField ARRAY<STRING>,\n"
                                + "    doubleArrayField ARRAY<DOUBLE>,\n"
                                + "    documentArrayField ARRAY<ROW<a STRING,b BIGINT>>,\n"
                                + "    minKeyField STRING,\n"
                                + "    maxKeyField STRING,\n"
                                + "    regexField STRING,\n"
                                + "    undefinedField STRING,\n"
                                + "    nullField STRING,\n"
                                + "    binaryField BINARY,\n"
                                + "    javascriptField STRING,\n"
                                + "    dbReferenceField ROW<$ref STRING,$id STRING>,\n"
                                + "    PRIMARY KEY (_id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s'"
                                + ")",
                        MONGODB_CONTAINER.getHostAndPort(),
                        FLINK_USER,
                        FLINK_USER_PASSWORD,
                        "column_type_test",
                        "full_types");

        String sinkDDL =
                "CREATE TABLE sink (\n"
                        + "    _id STRING,\n"
                        + "    stringField STRING,\n"
                        + "    uuidField STRING,\n"
                        + "    md5Field STRING,\n"
                        + "    timeField TIME,\n"
                        + "    dateField DATE,\n"
                        + "    dateBefore1970 DATE,\n"
                        + "    dateToTimestampField TIMESTAMP(3),\n"
                        + "    dateToLocalTimestampField TIMESTAMP_LTZ(3),\n"
                        + "    timestampField TIMESTAMP(0),\n"
                        + "    timestampToLocalTimestampField TIMESTAMP_LTZ(0),\n"
                        + "    booleanField BOOLEAN,\n"
                        + "    decimal128Field DECIMAL ,\n"
                        + "    doubleField DOUBLE,\n"
                        + "    int32field INT,\n"
                        + "    int64Field BIGINT,\n"
                        + "    documentField ROW<a STRING,b BIGINT>,\n"
                        + "    mapField MAP<STRING,MAP<STRING,INT>>,\n"
                        + "    arrayField ARRAY<STRING>,\n"
                        + "    doubleArrayField ARRAY<DOUBLE>,\n"
                        + "    documentArrayField ARRAY<ROW<a STRING,b BIGINT>>,\n"
                        + "    minKeyField STRING,\n"
                        + "    maxKeyField STRING,\n"
                        + "    regexField STRING,\n"
                        + "    undefinedField STRING,\n"
                        + "    nullField STRING,\n"
                        + "    binaryField BINARY,\n"
                        + "    javascriptField STRING,\n"
                        + "    dbReferenceField ROW<$ref STRING,$id STRING>\n"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT _id,\n"
                                + "stringField,\n"
                                + "uuidField,\n"
                                + "md5Field,\n"
                                + "timeField,\n"
                                + "dateField,\n"
                                + "dateBefore1970,\n"
                                + "dateToTimestampField,\n"
                                + "dateToLocalTimestampField,\n"
                                + "timestampField,\n"
                                + "timestampToLocalTimestampField,\n"
                                + "booleanField,\n"
                                + "decimal128Field,\n"
                                + "doubleField,\n"
                                + "int32field,\n"
                                + "int64Field,\n"
                                + "documentField,\n"
                                + "mapField,\n"
                                + "arrayField,\n"
                                + "doubleArrayField,\n"
                                + "documentArrayField,\n"
                                + "minKeyField,\n"
                                + "maxKeyField,\n"
                                + "regexField,\n"
                                + "undefinedField,\n"
                                + "nullField,\n"
                                + "binaryField,\n"
                                + "javascriptField,\n"
                                + "dbReferenceField\n"
                                + "FROM full_types");

        waitForSnapshotStarted("sink");

        MongoCollection<Document> fullTypes =
                getMongoDatabase("column_type_test").getCollection("full_types");

        fullTypes.updateOne(
                Filters.eq("_id", new ObjectId("5d505646cf6d4fe581014ab2")),
                Updates.set("int64Field", 510L));

        // 2021-09-03T18:36:04.123Z
        BsonDateTime updatedDateTime = new BsonDateTime(1630694164123L);
        // 2021-09-03T18:36:04Z
        BsonTimestamp updatedTimestamp = new BsonTimestamp(1630694164, 0);
        fullTypes.updateOne(
                Filters.eq("_id", new ObjectId("5d505646cf6d4fe581014ab2")),
                Updates.combine(
                        Updates.set("timeField", updatedDateTime),
                        Updates.set("dateField", updatedDateTime),
                        Updates.set("dateToTimestampField", updatedDateTime),
                        Updates.set("dateToLocalTimestampField", updatedDateTime),
                        Updates.set("timestampField", updatedTimestamp),
                        Updates.set("timestampToLocalTimestampField", updatedTimestamp)));

        waitForSinkSize("sink", 5);

        List<String> expected =
                Arrays.asList(
                        "+I(5d505646cf6d4fe581014ab2,hello,0bd1e27e-2829-4b47-8e21-dfef93da44e1,2078693f4c61ce3073b01be69ab76428,17:54:14,2019-08-11,1960-08-11,2019-08-11T17:54:14.692,2019-08-11T17:54:14.692Z,2019-08-11T17:47:44,2019-08-11T17:47:44Z,true,11,10.5,10,50,hello,50,{inner_map={key=234}},[hello, world],[1.0, 1.1, null],[hello0,51, hello1,53],MIN_KEY,MAX_KEY,/^H/i,null,null,[1, 2, 3],function() { x++; },ref_doc,5d505646cf6d4fe581014ab3)",
                        "-U(5d505646cf6d4fe581014ab2,hello,0bd1e27e-2829-4b47-8e21-dfef93da44e1,2078693f4c61ce3073b01be69ab76428,17:54:14,2019-08-11,1960-08-11,2019-08-11T17:54:14.692,2019-08-11T17:54:14.692Z,2019-08-11T17:47:44,2019-08-11T17:47:44Z,true,11,10.5,10,50,hello,50,{inner_map={key=234}},[hello, world],[1.0, 1.1, null],[hello0,51, hello1,53],MIN_KEY,MAX_KEY,/^H/i,null,null,[1, 2, 3],function() { x++; },ref_doc,5d505646cf6d4fe581014ab3)",
                        "+U(5d505646cf6d4fe581014ab2,hello,0bd1e27e-2829-4b47-8e21-dfef93da44e1,2078693f4c61ce3073b01be69ab76428,17:54:14,2019-08-11,1960-08-11,2019-08-11T17:54:14.692,2019-08-11T17:54:14.692Z,2019-08-11T17:47:44,2019-08-11T17:47:44Z,true,11,10.5,10,510,hello,50,{inner_map={key=234}},[hello, world],[1.0, 1.1, null],[hello0,51, hello1,53],MIN_KEY,MAX_KEY,/^H/i,null,null,[1, 2, 3],function() { x++; },ref_doc,5d505646cf6d4fe581014ab3)",
                        "-U(5d505646cf6d4fe581014ab2,hello,0bd1e27e-2829-4b47-8e21-dfef93da44e1,2078693f4c61ce3073b01be69ab76428,17:54:14,2019-08-11,1960-08-11,2019-08-11T17:54:14.692,2019-08-11T17:54:14.692Z,2019-08-11T17:47:44,2019-08-11T17:47:44Z,true,11,10.5,10,510,hello,50,{inner_map={key=234}},[hello, world],[1.0, 1.1, null],[hello0,51, hello1,53],MIN_KEY,MAX_KEY,/^H/i,null,null,[1, 2, 3],function() { x++; },ref_doc,5d505646cf6d4fe581014ab3)",
                        "+U(5d505646cf6d4fe581014ab2,hello,0bd1e27e-2829-4b47-8e21-dfef93da44e1,2078693f4c61ce3073b01be69ab76428,18:36:04,2021-09-03,1960-08-11,2021-09-03T18:36:04.123,2021-09-03T18:36:04.123Z,2021-09-03T18:36:04,2021-09-03T18:36:04Z,true,11,10.5,10,510,hello,50,{inner_map={key=234}},[hello, world],[1.0, 1.1, null],[hello0,51, hello1,53],MIN_KEY,MAX_KEY,/^H/i,null,null,[1, 2, 3],function() { x++; },ref_doc,5d505646cf6d4fe581014ab3)");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEquals(expected, actual);

        result.getJobClient().get().cancel().get();
    }

    private static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        while (sinkSize(sinkName) == 0) {
            Thread.sleep(100);
        }
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
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
}
