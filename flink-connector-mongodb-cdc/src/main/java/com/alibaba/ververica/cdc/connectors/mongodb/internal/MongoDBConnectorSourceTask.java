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

package com.alibaba.ververica.cdc.connectors.mongodb.internal;

import com.mongodb.kafka.connect.source.MongoSourceTask;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source Task that holds mongodb kafka connector's source task to perceive the end of the snapshot
 * phase.
 */
public class MongoDBConnectorSourceTask extends SourceTask {

    private static final String COPY_KEY = "copy";

    private static final String TRUE = "true";

    private final MongoSourceTask delegate;

    private final Field isCopyingField;

    private SourceRecord lastSnapshotRecord;

    private boolean isInSnapshotPhase = false;

    public MongoDBConnectorSourceTask() throws NoSuchFieldException {
        this.delegate = new MongoSourceTask();
        this.isCopyingField = MongoSourceTask.class.getDeclaredField("isCopying");
    }

    @Override
    public String version() {
        return delegate.version();
    }

    @Override
    public void initialize(SourceTaskContext context) {
        this.delegate.initialize(context);
        this.context = context;
    }

    @Override
    public void start(Map<String, String> props) {
        delegate.start(props);
        isInSnapshotPhase = isCopying();
    }

    @Override
    public void commit() throws InterruptedException {
        delegate.commit();
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        delegate.commitRecord(record);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
            throws InterruptedException {
        delegate.commitRecord(record, metadata);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> sourceRecords = delegate.poll();

        if (!isInSnapshotPhase) {
            return sourceRecords;
        }

        if (sourceRecords == null || sourceRecords.isEmpty()) {
            // No changing stream event comes and source task is finished copying, then exit the
            // snapshot phase
            if (!isCopying()) {
                SourceRecord markedLastSnapshotRecord = markLastSnapshotRecord(lastSnapshotRecord);
                lastSnapshotRecord = null;
                isInSnapshotPhase = false;
                return Collections.singletonList(markedLastSnapshotRecord);
            }
            return sourceRecords;
        }

        LinkedList<SourceRecord> linkedRecords = new LinkedList<>();
        for (SourceRecord current : sourceRecords) {
            if (isSnapshotRecord(current)) {
                if (lastSnapshotRecord != null) {
                    linkedRecords.add(lastSnapshotRecord);
                }
                lastSnapshotRecord = current;
            } else {
                // Received non-snapshot record, then exit the snapshot phase
                if (lastSnapshotRecord != null) {
                    linkedRecords.add(markLastSnapshotRecord(lastSnapshotRecord));
                    lastSnapshotRecord = null;
                    isInSnapshotPhase = false;
                }
                linkedRecords.add(current);
            }
        }

        return linkedRecords;
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    private SourceRecord markLastSnapshotRecord(SourceRecord record) {
        if (record == null) {
            return null;
        }
        Map<String, Object> markedOffset = new HashMap<>(record.sourceOffset());
        markedOffset.put(COPY_KEY, "last");

        return new SourceRecord(
                record.sourcePartition(),
                markedOffset,
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value());
    }

    private boolean isSnapshotRecord(SourceRecord sourceRecord) {
        return TRUE.equals(sourceRecord.sourceOffset().get(COPY_KEY));
    }

    private boolean isCopying() {
        isCopyingField.setAccessible(true);
        try {
            return ((AtomicBoolean) isCopyingField.get(delegate)).get();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("can not access isCopying field of SourceTask", e);
        }
    }
}
