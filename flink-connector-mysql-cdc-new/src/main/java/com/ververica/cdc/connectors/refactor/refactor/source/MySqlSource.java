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

package com.ververica.cdc.connectors.refactor.refactor.source;

import com.ververica.cdc.connectors.base.schema.BaseSchema;
import com.ververica.cdc.connectors.base.source.ChangeEventHybridSource;
import com.ververica.cdc.connectors.base.source.config.SourceConfigFactory;
import com.ververica.cdc.connectors.base.source.dialect.SnapshotEventDialect;
import com.ververica.cdc.connectors.base.source.dialect.StreamingEventDialect;
import com.ververica.cdc.connectors.base.source.offset.OffsetFactory;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;

/** A MySql CDC Connector Source. */
public class MySqlSource<T> extends ChangeEventHybridSource<T> {

    public MySqlSource(
            SourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            SnapshotEventDialect snapshotEventDialect,
            StreamingEventDialect streamingEventDialect,
            Validator validator,
            BaseSchema baseSchema) {
        super(
                configFactory,
                deserializationSchema,
                offsetFactory,
                snapshotEventDialect,
                streamingEventDialect,
                validator,
                baseSchema);
    }

    public MySqlSource(
            SourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            SnapshotEventDialect snapshotEventDialect,
            StreamingEventDialect streamingEventDialect,
            BaseSchema baseSchema) {
        super(
                configFactory,
                deserializationSchema,
                offsetFactory,
                snapshotEventDialect,
                streamingEventDialect,
                baseSchema);
    }
}
