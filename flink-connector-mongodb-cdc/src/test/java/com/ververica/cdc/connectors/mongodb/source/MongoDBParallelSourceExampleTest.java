/*
 * Copyright 2022 Ververica Inc.
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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;

/** Example Tests for {@link MongoDBSource}. */
public class MongoDBParallelSourceExampleTest extends MongoDBSourceTestBase {

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testMongoDBExampleSource() throws Exception {
        String database = ROUTER.executeCommandFileInSeparateDatabase("inventory");

        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts(ROUTER.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + ".products")
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
