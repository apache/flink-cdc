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

package com.ververica.cdc.connectors.mongodb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Ignore;
import org.junit.Test;

import static com.ververica.cdc.connectors.mongodb.LegacyMongoDBTestBase.MONGODB_CONTAINER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;

/** Example Tests for {@link MongoDBSource}. */
public class LegacyMongoDBSourceExampleTest extends MongoDBSourceTestBase {

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        String inventory = SHARD.executeCommandFileInSeparateDatabase("inventory");

        SourceFunction<String> sourceFunction =
                MongoDBSource.<String>builder()
                        .hosts(MONGODB_CONTAINER.getHostAndPort())
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .databaseList(inventory)
                        .collectionList(inventory + ".products")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print()
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MongoDB Snapshot + Change Stream Events");
    }
}
