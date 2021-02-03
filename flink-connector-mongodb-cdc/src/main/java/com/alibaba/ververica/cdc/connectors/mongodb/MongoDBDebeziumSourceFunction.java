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

package com.alibaba.ververica.cdc.connectors.mongodb;

import org.apache.flink.annotation.PublicEvolving;

import com.alibaba.ververica.cdc.connectors.mongodb.internal.MongoDBConnectorDebeziumChangeConsumer;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.internal.DebeziumChangeConsumer;
import com.alibaba.ververica.cdc.debezium.internal.DebeziumOffset;

import javax.annotation.Nullable;

import java.util.Properties;

/**
 * The {@link MongoDBDebeziumSourceFunction} is a streaming data source that pulls captured change
 * data from databases into Flink.
 *
 * <p>The source function participates in checkpointing and guarantees that no data is lost during a
 * failure, and that the computation processes elements "exactly once".
 *
 * <p>Note: currently, the source function can't run in multiple parallel instances.
 *
 * <p>Please refer to MongoDB Kafka Connector's documentation for the available configuration
 * properties: https://docs.mongodb.com/kafka-connector/current/kafka-source
 */
@PublicEvolving
public class MongoDBDebeziumSourceFunction<T> extends DebeziumSourceFunction<T> {

    private static final long serialVersionUID = -2840579426476622716L;

    public MongoDBDebeziumSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset) {
        super(deserializer, properties, specificOffset);
    }

    @Override
    protected DebeziumChangeConsumer<T> createConsumer(
            SourceContext<T> sourceContext, boolean isInDbSnapshotPhase) {
        return new MongoDBConnectorDebeziumChangeConsumer<>(
                sourceContext, deserializer, isInDbSnapshotPhase, this::reportError);
    }
}
