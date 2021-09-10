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

package com.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.kafka.connect.source.MongoSourceTask;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.data.Envelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.bson.json.JsonReader;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source Task that proxies mongodb kafka connector's {@link MongoSourceTask} to adapt to {@link
 * com.ververica.cdc.debezium.internal.DebeziumChangeFetcher}.
 */
public class MongoDBConnectorSourceTask extends SourceTask {

    private static final String COPY_KEY = "copy";

    private static final String TRUE = "true";

    private static final String CLUSTER_TIME_FIELD = "clusterTime";

    private final MongoSourceTask target;

    private final Field isCopyingField;

    private SourceRecord currentLastSnapshotRecord;

    private boolean isInSnapshotPhase = false;

    public MongoDBConnectorSourceTask() throws NoSuchFieldException {
        this.target = new MongoSourceTask();
        this.isCopyingField = MongoSourceTask.class.getDeclaredField("isCopying");
    }

    @Override
    public String version() {
        return target.version();
    }

    @Override
    public void initialize(SourceTaskContext context) {
        this.target.initialize(context);
        this.context = context;
    }

    @Override
    public void start(Map<String, String> props) {
        target.start(props);
        isInSnapshotPhase = isCopying();
    }

    @Override
    public void commit() throws InterruptedException {
        target.commit();
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        target.commitRecord(record);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
            throws InterruptedException {
        target.commitRecord(record, metadata);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> sourceRecords = target.poll();

        if (isInSnapshotPhase) {
            // Step1. Snapshot Phase
            List<SourceRecord> outSourceRecords = null;
            if (sourceRecords != null && !sourceRecords.isEmpty()) {
                outSourceRecords = new LinkedList<>();
                for (SourceRecord current : sourceRecords) {
                    markRecordTimestamp(current);
                    if (isSnapshotRecord(current)) {
                        markSnapshotRecord(current);
                        if (currentLastSnapshotRecord != null) {
                            outSourceRecords.add(currentLastSnapshotRecord);
                        }
                        // Keep the current last snapshot record.
                        // When exit snapshot phase, mark it as the last of all snapshot records.
                        currentLastSnapshotRecord = current;
                    } else {
                        // Snapshot Phase Ended, Condition 1:
                        // Received non-snapshot record, exit snapshot phase immediately.
                        if (currentLastSnapshotRecord != null) {
                            outSourceRecords.add(
                                    markLastSnapshotRecordOfAll(currentLastSnapshotRecord));
                            currentLastSnapshotRecord = null;
                            isInSnapshotPhase = false;
                        }
                        outSourceRecords.add(current);
                    }
                }
            } else {
                // Snapshot Phase Ended, Condition 2:
                // No changing stream event comes and source task is finished copying,
                // then exit the snapshot phase.
                if (!isCopying()) {
                    if (currentLastSnapshotRecord != null) {
                        outSourceRecords =
                                Collections.singletonList(
                                        markLastSnapshotRecordOfAll(currentLastSnapshotRecord));
                        currentLastSnapshotRecord = null;
                    }
                    isInSnapshotPhase = false;
                }
            }
            return outSourceRecords;
        } else {
            // Step2. Change Streaming Phase
            if (sourceRecords != null && !sourceRecords.isEmpty()) {
                for (SourceRecord current : sourceRecords) {
                    markRecordTimestamp(current);
                }
            }
            return sourceRecords;
        }
    }

    @Override
    public void stop() {
        target.stop();
    }

    private void markRecordTimestamp(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Struct source = new Struct(value.schema().field(Envelope.FieldName.SOURCE).schema());
        long timestamp = System.currentTimeMillis();
        if (value.schema().field(CLUSTER_TIME_FIELD) != null) {
            String clusterTime = value.getString(CLUSTER_TIME_FIELD);
            if (clusterTime != null) {
                timestamp = new JsonReader(clusterTime).readTimestamp().getTime() * 1000L;
            }
        }
        source.put(AbstractSourceInfo.TIMESTAMP_KEY, timestamp);
        value.put(Envelope.FieldName.SOURCE, source);
    }

    private void markSnapshotRecord(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        SnapshotRecord.TRUE.toSource(source);
    }

    private SourceRecord markLastSnapshotRecordOfAll(SourceRecord record) {
        final Struct value = (Struct) record.value();
        final Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        final SnapshotRecord snapshot = SnapshotRecord.fromSource(source);
        if (snapshot == SnapshotRecord.TRUE) {
            SnapshotRecord.LAST.toSource(source);
        }
        return record;
    }

    private boolean isSnapshotRecord(SourceRecord sourceRecord) {
        return TRUE.equals(sourceRecord.sourceOffset().get(COPY_KEY));
    }

    private boolean isCopying() {
        isCopyingField.setAccessible(true);
        try {
            return ((AtomicBoolean) isCopyingField.get(target)).get();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access isCopying field of SourceTask", e);
        }
    }
}
