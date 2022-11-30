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

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffsetFactory;
import com.ververica.cdc.connectors.mongodb.source.reader.MongoDBRecordEmitter;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;

/**
 * The MongoDB CDC Source based on FLIP-27 which supports parallel reading snapshot of collection
 * and then continue to capture data change from change stream.
 *
 * <pre>
 *     1. The source supports parallel capturing database(s) or collection(s) change.
 *     2. The source supports checkpoint in split level when read snapshot data.
 *     3. The source doesn't need apply any lock of MongoDB.
 * </pre>
 *
 * <pre>{@code
 * MongoDBSource
 *     .<String>builder()
 *     .hosts("localhost:27017")
 *     .databaseList("mydb")
 *     .collectionList("mydb.users")
 *     .username(username)
 *     .password(password)
 *     .deserializer(new JsonDebeziumDeserializationSchema())
 *     .build();
 * }</pre>
 *
 * <p>See {@link MongoDBSourceBuilder} for more details.
 *
 * @param <T> the output type of the source.
 */
@Internal
@Experimental
public class MongoDBSource<T> extends IncrementalSource<T, MongoDBSourceConfig> {

    private static final long serialVersionUID = 1L;

    MongoDBSource(
            MongoDBSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema) {
        super(
                configFactory,
                deserializationSchema,
                new ChangeStreamOffsetFactory(),
                new MongoDBDialect());
    }

    /**
     * Get a MongoDBSourceBuilder to build a {@link MongoDBSource}.
     *
     * @return a MongoDB parallel source builder.
     */
    @PublicEvolving
    public static <T> MongoDBSourceBuilder<T> builder() {
        return new MongoDBSourceBuilder<>();
    }

    @Override
    protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
        return new MongoDBRecordEmitter<>(
                deserializationSchema, sourceReaderMetrics, offsetFactory);
    }
}
