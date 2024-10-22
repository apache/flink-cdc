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
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.mongodb.factory.MongoDBDataSourceFactory;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

/**
 * Basic class for testing MySQL binlog source, this contains a MySQL container which enables
 * binlog.
 */
public class MongoDBPipelineITCase {
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void testInitialStartupMode() throws Exception {
        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts("10.34.7.192:27017")
                        .username("mongouser")
                        .password("mongopw")
                        .databaseList("mgdb") // set captured database, support regex
                        .collectionList("mgdb.customers");
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new MongoDBDataSource(configFactory).getEventSourceProvider();

        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
        env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        MongoDBDataSourceFactory.IDENTIFIER,
                        new EventTypeInfo())
                .setParallelism(2)
                .print();
        env.execute();
    }
}
