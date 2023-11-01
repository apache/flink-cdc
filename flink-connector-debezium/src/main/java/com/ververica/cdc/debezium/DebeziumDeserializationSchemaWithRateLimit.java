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

package com.ververica.cdc.debezium;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.rate.RateLimiter;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A rate limit implementation of {@link DebeziumDeserializationSchema} which wrapper original
 * Deserializer to control the data rate.
 */
public class DebeziumDeserializationSchemaWithRateLimit<T>
        implements DebeziumDeserializationSchema<T> {

    private static final long serialVersionUID = -1552143143265670603L;

    /** Original deserializer. */
    private final DebeziumDeserializationSchema<T> originalDeserializer;

    /** RateLimiter. */
    private final RateLimiter rateLimiter;

    /** When the binlog data is reached, the count rate needs to be reset. */
    private boolean reachBinlog = false;

    public DebeziumDeserializationSchemaWithRateLimit(
            DebeziumDeserializationSchema<T> originalDeserializer, RateLimiter rateLimiter) {
        this.originalDeserializer = originalDeserializer;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        Long timestamp = getMessageTimestamp(record);
        if (!reachBinlog && timestamp != null && timestamp > 0) {
            reachBinlog = true;
            rateLimiter.resetRate(1);
        }
        rateLimiter.acquire();
        originalDeserializer.deserialize(record, out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return originalDeserializer.getProducedType();
    }

    /** Return the timestamp when the change event, snapshot change event timestamp is zero. */
    public static Long getMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();
        if (schema.field(Envelope.FieldName.SOURCE) == null) {
            return null;
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source.schema().field(Envelope.FieldName.TIMESTAMP) == null) {
            return null;
        }

        return source.getInt64(Envelope.FieldName.TIMESTAMP);
    }
}
