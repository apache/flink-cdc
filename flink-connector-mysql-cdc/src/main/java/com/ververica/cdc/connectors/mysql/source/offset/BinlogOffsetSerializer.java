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

package com.ververica.cdc.connectors.mysql.source.offset;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/** Serializer implementation for a {@link BinlogOffset}. */
@Internal
public class BinlogOffsetSerializer {

    public static final BinlogOffsetSerializer INSTANCE = new BinlogOffsetSerializer();

    public byte[] serialize(BinlogOffset binlogOffset) throws IOException {
        // use JSON serialization
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsBytes(binlogOffset.getOffset());
    }

    public BinlogOffset deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> offset = objectMapper.readValue(bytes, Map.class);
        return new BinlogOffset(offset);
    }
}
