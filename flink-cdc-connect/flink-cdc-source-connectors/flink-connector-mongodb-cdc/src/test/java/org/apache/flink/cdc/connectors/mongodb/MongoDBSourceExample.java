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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Basic class for testing MongoDB binlog source, this contains a MySQL container which enables
 * binlog.
 */
public class MongoDBSourceExample {
    public static void main(String[] args) throws Exception {
        DebeziumDeserializationSchema deserializationSchema =
                new JsonDebeziumDeserializationSchema();

        MongoDBSource mongoSource =
                MongoDBSource.builder()
                        .hosts("10.34.7.192:27017")
                        .databaseList("mgdb") // set captured database, support regex
                        .collectionList("mgdb.customers") // set captured collections, support regex
                        .username("mongouser")
                        .password("mongopw")
                        .deserializer(deserializationSchema)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute("Print MongoDB Snapshot + Change Stream");
    }
}
