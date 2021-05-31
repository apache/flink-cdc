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

package com.alibaba.ververica.cdc.debezium.internal;

import org.apache.flink.annotation.Internal;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/** Consume debezium change events. */
@Internal
public class DebeziumChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {

    private final Handover handover;

    public DebeziumChangeConsumer(Handover handover) {
        this.handover = handover;
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<SourceRecord, SourceRecord>> events,
            RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> recordCommitter)
            throws InterruptedException {
        try {
            handover.produce(recordCommitter, events);
        } catch (Throwable e) {
            // Hold this exception in handover and trigger handler to exit
            handover.reportError(e);
        }
    }
}
