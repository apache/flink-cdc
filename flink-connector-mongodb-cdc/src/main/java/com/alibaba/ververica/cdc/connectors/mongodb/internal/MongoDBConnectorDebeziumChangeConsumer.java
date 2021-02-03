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

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.internal.DebeziumChangeConsumer;
import com.alibaba.ververica.cdc.debezium.internal.ErrorReporter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.json.JsonReader;

/**
 * A consumer that consumes mongodb change messages from {@link io.debezium.engine.DebeziumEngine}.
 *
 * @param <T> The type of elements produced by the consumer.
 */
@Internal
public class MongoDBConnectorDebeziumChangeConsumer<T> extends DebeziumChangeConsumer<T> {

    private static final String HEARTBEAT_TOPIC_NAME_DEFAULT = "__mongodb_heartbeats";

    private static final String COPY_KEY = "copy";

    private static final String CLUSTER_TIME_FIELD = "clusterTime";

    private static final String TRUE = "true";

    public MongoDBConnectorDebeziumChangeConsumer(
            SourceFunction.SourceContext<T> sourceContext,
            DebeziumDeserializationSchema<T> deserialization,
            boolean isInDbSnapshotPhase,
            ErrorReporter errorReporter) {
        super(
                sourceContext,
                deserialization,
                isInDbSnapshotPhase,
                errorReporter,
                HEARTBEAT_TOPIC_NAME_DEFAULT);
    }

    @Override
    protected boolean isSnapshotRecord(SourceRecord record) {
        return TRUE.equals(record.sourceOffset().get(COPY_KEY));
    }

    @Override
    protected Long getMessageTimestamp(SourceRecord record) {
        if (isHeartbeatEvent(record)) {
            return System.currentTimeMillis();
        }

        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();

        if (schema.field(CLUSTER_TIME_FIELD) == null) {
            return null;
        }

        String clusterTime = value.getString(CLUSTER_TIME_FIELD);
        if (clusterTime == null) {
            return null;
        }

        return new JsonReader(clusterTime).readTimestamp().getTime() * 1000L;
    }
}
