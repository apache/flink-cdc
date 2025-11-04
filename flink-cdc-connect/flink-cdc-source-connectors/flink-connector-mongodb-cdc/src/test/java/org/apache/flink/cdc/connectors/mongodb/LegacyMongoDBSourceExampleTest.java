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

package org.apache.flink.cdc.connectors.mongodb;

import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.LegacyMongoDBContainer.FLINK_USER_PASSWORD;

/** Example Tests for {@link MongoDBSource}. */
class LegacyMongoDBSourceExampleTest extends LegacyMongoDBSourceTestBase {

    @Test
    @Disabled("Test ignored because it won't stop and is used for manual test")
    void testConsumingAllEvents() throws Exception {
        String inventory = ROUTER.executeCommandFileInSeparateDatabase("inventory");

        SourceFunction<String> sourceFunction =
                MongoDBSource.<String>builder()
                        .hosts(ROUTER.getHostAndPort())
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
