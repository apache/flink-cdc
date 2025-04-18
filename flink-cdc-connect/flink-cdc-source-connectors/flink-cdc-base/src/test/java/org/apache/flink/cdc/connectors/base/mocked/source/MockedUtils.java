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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import javax.annotation.Nullable;

import java.util.Map;

import static java.util.Collections.singletonMap;

/** Some utilities for mocked utilities. */
public class MockedUtils {

    public static final Schema KEY_SCHEMA =
            SchemaBuilder.struct()
                    .name("mocked_key_schema")
                    .field("id", Schema.INT64_SCHEMA)
                    .build();
    public static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .name("mocked_value_schema")
                    .field("op", Schema.STRING_SCHEMA)
                    .field("id", Schema.INT64_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("timestamp", Schema.INT64_SCHEMA)
                    .build();

    public static Struct createKeySchema(long id) {
        Struct struct = new Struct(KEY_SCHEMA);
        struct.put("id", id);
        return struct;
    }

    public static Struct createValueSchema(
            @Nullable Long timestamp, RowKind opType, long id, StringData name) {
        return createValueSchema(timestamp, opType, id, name.toString());
    }

    public static Struct createValueSchema(
            @Nullable Long timestamp, RowKind opType, long id, String name) {
        Struct struct = new Struct(VALUE_SCHEMA);
        struct.put("op", opType.shortString());
        struct.put("id", id);
        struct.put("name", name);

        if (timestamp != null) {
            struct.put("timestamp", timestamp);
        } else {
            struct.put("timestamp", Long.MIN_VALUE);
        }
        return struct;
    }

    public static Map<String, String> createWatermarkPartitionMap(String partition) {
        return singletonMap("mocked_ns", partition);
    }
}
