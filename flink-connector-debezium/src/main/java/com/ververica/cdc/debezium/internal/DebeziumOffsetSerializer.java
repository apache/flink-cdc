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

package com.ververica.cdc.debezium.internal;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** Serializer implementation for a {@link DebeziumOffset}. */
@Internal
public class DebeziumOffsetSerializer {
    public static final DebeziumOffsetSerializer INSTANCE = new DebeziumOffsetSerializer();

    public byte[] serialize(DebeziumOffset debeziumOffset) throws IOException {
        // we currently use JSON serialization for simplification, as the state is very small.
        // we can improve this in the future if needed
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsBytes(debeziumOffset);
    }

    public DebeziumOffset deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(bytes, DebeziumOffset.class);
    }
}
