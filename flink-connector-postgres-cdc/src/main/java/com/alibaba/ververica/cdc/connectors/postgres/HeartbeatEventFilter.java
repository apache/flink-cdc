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

package com.alibaba.ververica.cdc.connectors.postgres;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.heartbeat.Heartbeat;
import org.apache.kafka.connect.source.SourceRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DebeziumDeserializationSchema} which wraps a real {@link DebeziumDeserializationSchema}
 * to drop heartbeat events.
 *
 * @see Heartbeat
 */
public class HeartbeatEventFilter<T> implements DebeziumDeserializationSchema<T> {
    private static final long serialVersionUID = -4450118969976653497L;

    private final String heartbeatTopicPrefix;
    private final DebeziumDeserializationSchema<T> serializer;

    public HeartbeatEventFilter(
            String heartbeatTopicPrefix, DebeziumDeserializationSchema<T> serializer) {
        this.heartbeatTopicPrefix = checkNotNull(heartbeatTopicPrefix);
        this.serializer = checkNotNull(serializer);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        String topic = record.topic();
        if (topic != null && topic.startsWith(heartbeatTopicPrefix)) {
            // drop heartbeat events
            return;
        }
        serializer.deserialize(record, out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return serializer.getProducedType();
    }
}
