/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.triggerFailover;
import static org.junit.Assert.assertArrayEquals;

/** Testing whether duplicate row data were emitted during snapshot failover. */
@RunWith(Parameterized.class)
public class MongoDBNoDuplicateSnapshotITCase extends MongoDBSourceTestBase {

    public MongoDBNoDuplicateSnapshotITCase(
            boolean parallelismSnapshot,
            boolean fullChangelogEnabled,
            MongoDBTestUtils.FailoverType failOverType) {
        this.parallelismSnapshotEnabled = parallelismSnapshot;
        this.fullChangelogEnabled = fullChangelogEnabled;
        this.failOverType = failOverType;
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    public static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private static final Logger LOG =
            LoggerFactory.getLogger(MongoDBNoDuplicateSnapshotITCase.class);

    private final int dataCount = 1000;

    private final boolean parallelismSnapshotEnabled;
    private final boolean fullChangelogEnabled;
    private final MongoDBTestUtils.FailoverType failOverType;

    @Parameterized.Parameters(name = "parallelSnapshot: {0}, fullChangelog: {1}, failType: {2}")
    public static Object[] parameters() {
        return new Object[][] {
            new Object[] {false, false, MongoDBTestUtils.FailoverType.TM},
            new Object[] {false, false, MongoDBTestUtils.FailoverType.JM},
            new Object[] {false, true, MongoDBTestUtils.FailoverType.TM},
            new Object[] {false, true, MongoDBTestUtils.FailoverType.JM},
            new Object[] {true, false, MongoDBTestUtils.FailoverType.TM},
            new Object[] {true, false, MongoDBTestUtils.FailoverType.JM},
            new Object[] {true, true, MongoDBTestUtils.FailoverType.TM},
            new Object[] {true, true, MongoDBTestUtils.FailoverType.JM}
        };
    }

    @Test
    public void testNoDuplicateDuringSnapshotFailover() throws Exception {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < dataCount; i++) {
            sb.append(String.format("{ 'key': 'value%d' },", i));
        }

        String collectionName = "test_col";

        CONTAINER.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");
        String dbName =
                CONTAINER.executeCommandInSeparateDatabase(
                        String.format(
                                "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                                collectionName, collectionName),
                        "dupchk");
        CONTAINER.executeCommandInDatabase(
                "db.getCollection('" + collectionName + "')" + ".insertMany([" + sb + "])", dbName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        String sourceDDL =
                String.format(
                        "CREATE TABLE test_col ("
                                + " _id STRING NOT NULL,"
                                + " key STRING,"
                                + " primary key (_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'scan.full-changelog' = '%s'"
                                + ")",
                        CONTAINER.getHostAndPort(),
                        FLINK_USER,
                        FLINK_USER_PASSWORD,
                        dbName,
                        "test_col",
                        parallelismSnapshotEnabled,
                        fullChangelogEnabled);

        tEnv.executeSql(sourceDDL);

        TableResult tableResult = tEnv.executeSql("select key from test_col");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().get().getJobID();

        if (iterator.hasNext()) {
            triggerFailover(
                    failOverType, jobId, miniClusterResource.getMiniCluster(), () -> sleepMs(100));
        }

        List<String> result = fetchRows(iterator, dataCount);
        Collections.sort(result);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < dataCount; i++) {
            expected.add(String.format("+I[value%d]", i));
        }
        Collections.sort(expected);

        assertArrayEquals(expected.toArray(), result.toArray());
    }
}
