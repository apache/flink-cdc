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

import com.ververica.cdc.debezium.utils.SourceRecordUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A simple implementation of {@link DebeziumDeserializationSchema} which converts the received
 * {@link SourceRecord} into String.
 */
public class StringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = -3168848963265670603L;
    private final int timeZoneOffset;

    public StringDebeziumDeserializationSchema() {
        this.timeZoneOffset = 8;
    }

    public StringDebeziumDeserializationSchema(int timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) {
        SourceRecord sourceRecord = SourceRecordUtil.correctTimeZoneSourceRecord(record, timeZoneOffset);
        out.collect(sourceRecord.toString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
