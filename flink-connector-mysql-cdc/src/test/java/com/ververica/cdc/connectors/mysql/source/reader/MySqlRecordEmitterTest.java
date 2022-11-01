package com.ververica.cdc.connectors.mysql.source.reader;

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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Collector;

import com.ververica.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.heartbeat.Heartbeat;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Unit test for {@link MySqlRecordEmitter}. */
public class MySqlRecordEmitterTest {

    @Test
    public void testHeartbeatEventHandling() throws Exception {
        Heartbeat heartbeat = Heartbeat.create(Duration.ofMillis(100), "fake-topic", "fake-key");
        BinlogOffset fakeOffset = BinlogOffset.ofBinlogFilePosition("fake-file", 15213L);
        MySqlRecordEmitter<Void> recordEmitter = createRecordEmitter();
        MySqlBinlogSplitState splitState = createBinlogSplitState();
        heartbeat.forcedBeat(
                Collections.emptyMap(),
                fakeOffset.getOffset(),
                record -> {
                    try {
                        recordEmitter.emitRecord(
                                SourceRecords.fromSingleRecord(record),
                                new TestingReaderOutput<>(),
                                splitState);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to emit heartbeat record", e);
                    }
                });
        assertNotNull(splitState.getStartingOffset());
        assertEquals(0, splitState.getStartingOffset().compareTo(fakeOffset));
    }

    private MySqlRecordEmitter<Void> createRecordEmitter() {
        return new MySqlRecordEmitter<>(
                new DebeziumDeserializationSchema<Void>() {
                    @Override
                    public void deserialize(SourceRecord record, Collector<Void> out) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public TypeInformation<Void> getProducedType() {
                        return TypeInformation.of(Void.class);
                    }
                },
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                false);
    }

    private MySqlBinlogSplitState createBinlogSplitState() {
        return new MySqlBinlogSplitState(
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofEarliest(),
                        BinlogOffset.ofNonStopping(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0));
    }
}
