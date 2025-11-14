/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Collector;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.debezium.config.CommonConnectorConfig.TRANSACTION_TOPIC;
import static io.debezium.connector.mysql.MySqlConnectorConfig.SERVER_NAME;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.reader.MySqlRecordEmitter}. */
class MySqlRecordEmitterTest {

    @Test
    void testHeartbeatEventHandling() throws Exception {
        Configuration dezConf =
                JdbcConfiguration.create()
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 100)
                        .with(TRANSACTION_TOPIC, "fake-topic")
                        .with(SERVER_NAME, "mysql_binlog_source")
                        .build();

        MySqlConnectorConfig mySqlConfig = new MySqlConnectorConfig(dezConf);
        HeartbeatFactory<TableId> heartbeatFactory =
                new HeartbeatFactory<>(
                        new MySqlConnectorConfig(dezConf),
                        TopicSelector.defaultSelector(
                                mySqlConfig, (id, prefix, delimiter) -> "fake-topic"),
                        SchemaNameAdjuster.create());
        Heartbeat heartbeat = heartbeatFactory.createHeartbeat();
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
        heartbeat.close();
        Assertions.assertThat(splitState.getStartingOffset())
                .isNotNull()
                .isEqualByComparingTo(fakeOffset);
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
