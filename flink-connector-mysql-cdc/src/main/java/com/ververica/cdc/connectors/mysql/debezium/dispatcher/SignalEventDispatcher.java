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

package com.ververica.cdc.connectors.mysql.debezium.dispatcher;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/**
 * A dispatcher to dispatch watermark signal events.
 *
 * <p>The watermark signal event is used to describe the start point and end point of a split scan.
 * The Watermark Signal Algorithm is inspired by https://arxiv.org/pdf/2010.12597v1.pdf.
 */
public class SignalEventDispatcher {

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    public static final String DATABASE_NAME = "db";
    public static final String TABLE_NAME = "table";
    public static final String WATERMARK_SIGNAL = "_split_watermark_signal_";
    public static final String SPLIT_ID_KEY = "split_id";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String WATERMARK_KIND = "watermark_kind";
    public static final String SIGNAL_EVENT_KEY_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.watermark.key";
    public static final String SIGNAL_EVENT_VALUE_SCHEMA_NAME =
            "io.debezium.connector.flink.cdc.embedded.watermark.value";

    private final Schema signalEventKeySchema;
    private final Schema signalEventValueSchema;
    private final Map<String, ?> sourcePartition;
    private final String topic;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public SignalEventDispatcher(
            Map<String, ?> sourcePartition, String topic, ChangeEventQueue<DataChangeEvent> queue) {
        this.sourcePartition = sourcePartition;
        this.topic = topic;
        this.queue = queue;
        this.signalEventKeySchema =
                SchemaBuilder.struct()
                        .name(SCHEMA_NAME_ADJUSTER.adjust(SIGNAL_EVENT_KEY_SCHEMA_NAME))
                        .field(SPLIT_ID_KEY, Schema.STRING_SCHEMA)
                        .field(WATERMARK_SIGNAL, Schema.BOOLEAN_SCHEMA)
                        .build();
        this.signalEventValueSchema =
                SchemaBuilder.struct()
                        .name(SCHEMA_NAME_ADJUSTER.adjust(SIGNAL_EVENT_VALUE_SCHEMA_NAME))
                        .field(SPLIT_ID_KEY, Schema.STRING_SCHEMA)
                        .field(WATERMARK_KIND, Schema.STRING_SCHEMA)
                        .build();
    }

    public void dispatchWatermarkEvent(
            MySqlSplit mySqlSplit, BinlogOffset watermark, WatermarkKind watermarkKind)
            throws InterruptedException {

        SourceRecord sourceRecord =
                new SourceRecord(
                        sourcePartition,
                        watermark.getOffset(),
                        topic,
                        signalEventKeySchema,
                        signalRecordKey(mySqlSplit.splitId()),
                        signalEventValueSchema,
                        signalRecordValue(mySqlSplit.splitId(), watermarkKind));
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }

    private Struct signalRecordKey(String splitId) {
        Struct result = new Struct(signalEventKeySchema);
        result.put(SPLIT_ID_KEY, splitId);
        result.put(WATERMARK_SIGNAL, true);
        return result;
    }

    private Struct signalRecordValue(String splitId, WatermarkKind watermarkKind) {
        Struct result = new Struct(signalEventValueSchema);
        result.put(SPLIT_ID_KEY, splitId);
        result.put(WATERMARK_KIND, watermarkKind.toString());
        return result;
    }

    /** The watermark kind. */
    public enum WatermarkKind {
        LOW,
        HIGH,
        BINLOG_END;

        public WatermarkKind fromString(String kindString) {
            if (LOW.name().equalsIgnoreCase(kindString)) {
                return LOW;
            } else if (HIGH.name().equalsIgnoreCase(kindString)) {
                return HIGH;
            } else {
                return BINLOG_END;
            }
        }
    }
}
