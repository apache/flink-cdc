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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH;

/** Example Tests for {@link MongoDBSource}. */
class MongoDBParallelSourceExampleTest extends MongoDBSourceTestBase {

    @Test
    @Disabled("Test ignored because it won't stop and is used for manual test")
    void testMongoDBExampleSource() throws Exception {
        String database = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("inventory");

        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts(MONGO_CONTAINER.getHostAndPort())
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
