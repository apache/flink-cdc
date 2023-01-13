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

package com.ververica.cdc.connectors.mongodb.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplitState;
import com.ververica.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isHighWatermarkEvent;
import static com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isWatermarkEvent;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.getFetchTimestamp;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.getMessageTimestamp;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.getResumeToken;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.isDataChangeRecord;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.isHeartbeatEvent;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 */
public final class MongoDBRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBRecordEmitter.class);

    public MongoDBRecordEmitter(
            DebeziumDeserializationSchema<T> deserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            OffsetFactory offsetFactory) {
        super(deserializationSchema, sourceReaderMetrics, false, offsetFactory);
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = getOffsetPosition(element);
            if (isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isHeartbeatEvent(element)) {
            if (splitState.isStreamSplitState()) {
                updatePositionForStreamSplit(element, splitState);
            }
        } else if (isDataChangeRecord(element)) {
            if (splitState.isStreamSplitState()) {
                updatePositionForStreamSplit(element, splitState);
            }
            reportMetrics(element);
            emitElement(element, output);
        } else {
            // unknown element
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

    private void updatePositionForStreamSplit(SourceRecord element, SourceSplitState splitState) {
        BsonDocument resumeToken = getResumeToken(element);
        StreamSplitState streamSplitState = splitState.asStreamSplitState();
        ChangeStreamOffset offset = (ChangeStreamOffset) streamSplitState.getStartingOffset();
        if (offset != null) {
            offset.updatePosition(resumeToken);
        }
        splitState.asStreamSplitState().setStartingOffset(offset);
    }

    @Override
    protected void reportMetrics(SourceRecord element) {
        long now = System.currentTimeMillis();
        // record the latest process time
        sourceReaderMetrics.recordProcessTime(now);
        Long messageTimestamp = getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = getFetchTimestamp(element);
            if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
            // report emit delay
            sourceReaderMetrics.recordEmitDelay(now - messageTimestamp);
        }
    }
}
