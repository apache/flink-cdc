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
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;

/** Example Tests for {@link MongoDBSource}. */
@RunWith(Parameterized.class)
public class MongoDBParallelSourceExampleTest extends MongoDBSourceTestBase {

    @Parameterized.Parameters(name = "mongoVersion: {0} parallelismSnapshot: {1}")
    public static Object[] parameters() {
        List<Object[]> parameterTuples = new ArrayList<>();
        for (String mongoVersion : MONGO_VERSIONS) {
            parameterTuples.add(new Object[] {mongoVersion, true});
            parameterTuples.add(new Object[] {mongoVersion, false});
        }
        return parameterTuples.toArray();
    }

    public MongoDBParallelSourceExampleTest(String mongoVersion) {
        super(mongoVersion);
    }

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testMongoDBExampleSource() throws Exception {
        String database = mongoContainer.executeCommandFileInSeparateDatabase("inventory");

        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts(mongoContainer.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + ".products")
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .closeIdleReaders(true)
                        .build();

        Configuration config = new Configuration();
        config.set(ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBParallelSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print MongoDB Snapshot + Change Stream");
    }
}
