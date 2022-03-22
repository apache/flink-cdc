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

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/** Utils to deal with OceanBase SourceRecord schema. */
public class OceanBaseSchemaUtils {

    public static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field("tenant", Schema.STRING_SCHEMA)
                .field("database", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_STRING_SCHEMA)
                .field("unique_id", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    public static Struct sourceStruct(
            String tenant, String database, String table, String timestamp, String uniqueId) {
        Struct struct =
                new Struct(sourceSchema())
                        .put("tenant", tenant)
                        .put("database", database)
                        .put("table", table);
        if (timestamp != null) {
            struct.put("timestamp", timestamp);
        }
        if (uniqueId != null) {
            struct.put("unique_id", uniqueId);
        }
        return struct;
    }
}
