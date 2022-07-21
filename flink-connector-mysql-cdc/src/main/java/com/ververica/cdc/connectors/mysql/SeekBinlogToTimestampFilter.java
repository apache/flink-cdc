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

package com.ververica.cdc.connectors.mysql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DebeziumDeserializationSchema} which wraps a real {@link DebeziumDeserializationSchema}
 * to seek binlog to the specific timestamp.
 */
public class SeekBinlogToTimestampFilter<T> implements DebeziumDeserializationSchema<T> {
    private static final long serialVersionUID = -4450118969976653497L;
    protected static final Logger LOG = LoggerFactory.getLogger(SeekBinlogToTimestampFilter.class);

    private final long startupTimestampMillis;
    private final DebeziumDeserializationSchema<T> serializer;

    private transient boolean find = false;
    private transient long filtered = 0L;

    public SeekBinlogToTimestampFilter(
            long startupTimestampMillis, DebeziumDeserializationSchema<T> serializer) {
        this.startupTimestampMillis = startupTimestampMillis;
        this.serializer = serializer;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        if (find) {
            serializer.deserialize(record, out);
            return;
        }

        if (filtered == 0) {
            LOG.info("Begin to seek binlog to the specific timestamp {}.", startupTimestampMillis);
        }

        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Long ts = source.getInt64(Envelope.FieldName.TIMESTAMP);
        if (ts != null && ts >= startupTimestampMillis) {
            serializer.deserialize(record, out);
            find = true;
            LOG.info(
                    "Successfully seek to the specific timestamp {} with filtered {} change events.",
                    startupTimestampMillis,
                    filtered);
        } else {
            filtered++;
            if (filtered % 10000 == 0) {
                LOG.info(
                        "Seeking binlog to specific timestamp with filtered {} change events.",
                        filtered);
            }
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return serializer.getProducedType();
    }
}
