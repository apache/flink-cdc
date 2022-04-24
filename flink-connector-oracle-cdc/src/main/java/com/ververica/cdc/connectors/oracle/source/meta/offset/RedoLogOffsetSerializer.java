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

package com.ververica.cdc.connectors.oracle.source.meta.offset;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/** Serializer implementation for a {@link RedoLogOffset}. */
@Internal
public class RedoLogOffsetSerializer {

    public static final RedoLogOffsetSerializer INSTANCE = new RedoLogOffsetSerializer();

    public byte[] serialize(RedoLogOffset redoLogOffset) throws IOException {
        // use JSON serialization
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsBytes(redoLogOffset.getOffset());
    }

    public RedoLogOffset deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> offset = objectMapper.readValue(bytes, Map.class);
        return new RedoLogOffset(offset);
    }
}
