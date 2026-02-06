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

package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;

/** TiDBSourceInfoStructMaker. */
public class TiDBSourceInfoStructMaker implements SourceInfoStructMaker<TiDBSourceInfo> {
    private final Schema schema;

    public TiDBSourceInfoStructMaker() {
        this.schema =
                SchemaBuilder.struct()
                        .field(TiDBSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(TiDBSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                        .field(TiDBSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(TiDBSourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(TiDBSourceInfo.COMMIT_VERSION_KEY, Schema.INT64_SCHEMA)
                        .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(TiDBSourceInfo sourceInfo) {
        Struct source = new Struct(schema);
        source.put(TiDBSourceInfo.TABLE_NAME_KEY, sourceInfo.table());
        Instant timestamp = sourceInfo.timestamp();
        long commitVersion = sourceInfo.getCommitVersion();
        source.put(TiDBSourceInfo.TIMESTAMP_KEY, timestamp != null ? timestamp.toEpochMilli() : 0);
        // todo timestamp to commit version.
        source.put(TiDBSourceInfo.COMMIT_VERSION_KEY, commitVersion);
        if (sourceInfo.database() != null) {
            source.put(TiDBSourceInfo.DATABASE_NAME_KEY, sourceInfo.database());
        }
        return source;
    }
}
