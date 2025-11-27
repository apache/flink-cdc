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
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
          new DebeziumDeserializationSchema<>() {
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
                false,
                false,
          false);
    }

    @Test
    void testTransactionMetadataEventsDisabledByDefault() throws Exception {
        SourceRecord transactionBeginEvent = createTransactionMetadataEvent("BEGIN", "tx-123", 100L);

        Assertions.assertThat(RecordUtils.isTransactionMetadataEvent(transactionBeginEvent))
                .isTrue();

        AtomicInteger emittedRecordsCount = new AtomicInteger(0);
        MySqlRecordEmitter<String> recordEmitter = createRecordEmitterWithTransactionConfig(emittedRecordsCount, false);
        MySqlBinlogSplitState splitState = createBinlogSplitState();

        BinlogOffset offsetBeforeEmit = splitState.getStartingOffset();

        TestingReaderOutput<String> readerOutput = new TestingReaderOutput<>();
        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(transactionBeginEvent),
                readerOutput,
                splitState);

        // Verify the offset was updated (this should always happen)
        BinlogOffset expectedOffset = RecordUtils.getBinlogPosition(transactionBeginEvent);
        Assertions.assertThat(splitState.getStartingOffset())
                .isNotNull()
                .isNotEqualTo(offsetBeforeEmit)
                .isEqualByComparingTo(expectedOffset);

        // Verify the event was NOT emitted (because includeTransactionMetadataEvents=false)
        Assertions.assertThat(emittedRecordsCount.get()).isEqualTo(0);
        Assertions.assertThat(readerOutput.getEmittedRecords()).isEmpty();
    }

    @Test
    void testTransactionMetadataEventsEnabledExplicitly() throws Exception {
        SourceRecord transactionBeginEvent = createTransactionMetadataEvent("BEGIN", "tx-456", 150L);

        Assertions.assertThat(RecordUtils.isTransactionMetadataEvent(transactionBeginEvent))
                .isTrue();

        AtomicInteger emittedRecordsCount = new AtomicInteger(0);
        MySqlRecordEmitter<String> recordEmitter = createRecordEmitterWithTransactionConfig(emittedRecordsCount, true);
        MySqlBinlogSplitState splitState = createBinlogSplitState();

        BinlogOffset offsetBeforeEmit = splitState.getStartingOffset();

        TestingReaderOutput<String> readerOutput = new TestingReaderOutput<>();
        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(transactionBeginEvent),
                readerOutput,
                splitState);

        // Verify the offset was updated
        BinlogOffset expectedOffset = RecordUtils.getBinlogPosition(transactionBeginEvent);
        Assertions.assertThat(splitState.getStartingOffset())
                .isNotNull()
                .isNotEqualTo(offsetBeforeEmit)
                .isEqualByComparingTo(expectedOffset);

        // Verify the event was emitted (because includeTransactionMetadataEvents=true)
        Assertions.assertThat(emittedRecordsCount.get()).isEqualTo(1);
        Assertions.assertThat(readerOutput.getEmittedRecords()).hasSize(1);
    }

    @Test
    void testTransactionBeginEventHandling() throws Exception {
        SourceRecord transactionBeginEvent = createTransactionMetadataEvent("BEGIN", "tx-123", 100L);

        Assertions.assertThat(RecordUtils.isTransactionMetadataEvent(transactionBeginEvent))
                .isTrue();

        AtomicInteger emittedRecordsCount = new AtomicInteger(0);
        MySqlRecordEmitter<String> recordEmitter = createRecordEmitterWithCounter(emittedRecordsCount);
        MySqlBinlogSplitState splitState = createBinlogSplitState();

        BinlogOffset offsetBeforeEmit = splitState.getStartingOffset();

        TestingReaderOutput<String> readerOutput = new TestingReaderOutput<>();
        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(transactionBeginEvent),
                readerOutput,
                splitState);

        // Verify the offset was updated
        BinlogOffset expectedOffset = RecordUtils.getBinlogPosition(transactionBeginEvent);
        Assertions.assertThat(splitState.getStartingOffset())
                .isNotNull()
                .isNotEqualTo(offsetBeforeEmit)
                .isEqualByComparingTo(expectedOffset);

        // Verify the event was emitted
        Assertions.assertThat(emittedRecordsCount.get()).isEqualTo(1);
    }

    @Test
    void testTransactionEndEventHandling() throws Exception {
        SourceRecord transactionEndEvent = createTransactionMetadataEvent("END", "tx-123", 200L);

        Assertions.assertThat(RecordUtils.isTransactionMetadataEvent(transactionEndEvent))
                .isTrue();

        AtomicInteger emittedRecordsCount = new AtomicInteger(0);
        MySqlRecordEmitter<String> recordEmitter = createRecordEmitterWithCounter(emittedRecordsCount);
        MySqlBinlogSplitState splitState = createBinlogSplitState();

        TestingReaderOutput<String> readerOutput = new TestingReaderOutput<>();
        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(transactionEndEvent),
                readerOutput,
                splitState);

        // Verify the offset was updated
        BinlogOffset expectedOffset = RecordUtils.getBinlogPosition(transactionEndEvent);
        Assertions.assertThat(splitState.getStartingOffset())
                .isNotNull()
                .isEqualByComparingTo(expectedOffset);

        // Verify the event was emitted
        Assertions.assertThat(emittedRecordsCount.get()).isEqualTo(1);
    }


    @Test
    void testNonTransactionEventNotDetected() {
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("op", Schema.STRING_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", 1);
        Struct value = new Struct(valueSchema).put("op", "c");

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", 100L);

        SourceRecord dataRecord = new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset,
                "test.table",
                keySchema,
                key,
                valueSchema,
                value);

        // Verify it's NOT detected as a transaction metadata event
        Assertions.assertThat(RecordUtils.isTransactionMetadataEvent(dataRecord)).isFalse();
    }

    @Test
    void testTransactionEventWithoutKeySchemaNotDetected() {
        Schema valueSchema = SchemaBuilder.struct()
                .name(RecordUtils.SCHEMA_TRANSACTION_METADATA_EVENT_KEY_NAME)
                .field("status", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema).put("status", "BEGIN");

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", 100L);

        SourceRecord record = new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset,
                "transaction.topic",
                null, // No key schema
                null,
                valueSchema,
                value);

        // Verify it's NOT detected as a transaction metadata event
        Assertions.assertThat(RecordUtils.isTransactionMetadataEvent(record)).isFalse();
    }

    @Test
    void testMultipleTransactionEventsWithDisabledConfig() throws Exception {
        SourceRecord beginEvent = createTransactionMetadataEvent("BEGIN", "tx-789", 300L);
        SourceRecord endEvent = createTransactionMetadataEvent("END", "tx-789", 400L);

        AtomicInteger emittedRecordsCount = new AtomicInteger(0);
        MySqlRecordEmitter<String> recordEmitter = createRecordEmitterWithTransactionConfig(emittedRecordsCount, false);
        MySqlBinlogSplitState splitState = createBinlogSplitState();

        TestingReaderOutput<String> readerOutput = new TestingReaderOutput<>();
        
        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(beginEvent),
                readerOutput,
                splitState);

        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(endEvent),
                readerOutput,
                splitState);

        // Verify offsets were updated but no events were emitted
        BinlogOffset expectedOffset = RecordUtils.getBinlogPosition(endEvent);
        Assertions.assertThat(splitState.getStartingOffset())
                .isNotNull()
                .isEqualByComparingTo(expectedOffset);

        // Verify no events were emitted (because includeTransactionMetadataEvents=false)
        Assertions.assertThat(emittedRecordsCount.get()).isEqualTo(0);
        Assertions.assertThat(readerOutput.getEmittedRecords()).isEmpty();
    }

    @Test 
    void testMixedEventsWithTransactionMetadataDisabled() throws Exception {
        SourceRecord transactionEvent = createTransactionMetadataEvent("BEGIN", "tx-mixed", 500L);
        SourceRecord dataEvent = createDataChangeEvent("test.table", 501L);

        AtomicInteger emittedRecordsCount = new AtomicInteger(0);
        MySqlRecordEmitter<String> recordEmitter = createRecordEmitterWithTransactionConfig(emittedRecordsCount, false);
        MySqlBinlogSplitState splitState = createBinlogSplitState();

        TestingReaderOutput<String> readerOutput = new TestingReaderOutput<>();

        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(transactionEvent),
                readerOutput,
                splitState);

        recordEmitter.emitRecord(
                SourceRecords.fromSingleRecord(dataEvent),
                readerOutput,
                splitState);

        // Verify only data event was emitted (count=1, not 2)
        Assertions.assertThat(emittedRecordsCount.get()).isEqualTo(1);
        Assertions.assertThat(readerOutput.getEmittedRecords()).hasSize(1);
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

    /**
     * Helper method to create a MySqlRecordEmitter that counts emitted records.
     */
    private MySqlRecordEmitter<String> createRecordEmitterWithCounter(AtomicInteger counter) {
        return createRecordEmitterWithTransactionConfig(counter, true);
    }

    /**
     * Helper method to create a MySqlRecordEmitter with configurable transaction metadata events.
     */
    private MySqlRecordEmitter<String> createRecordEmitterWithTransactionConfig(AtomicInteger counter, boolean includeTransactionMetadataEvents) {
        return new MySqlRecordEmitter<>(
          new DebeziumDeserializationSchema<>() {
              @Override
              public void deserialize(SourceRecord record, Collector<String> out) {
                  counter.incrementAndGet();
                  out.collect("transaction-event");
              }

              @Override
              public TypeInformation<String> getProducedType() {
                  return TypeInformation.of(String.class);
              }
          },
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                false,
                includeTransactionMetadataEvents,
          false);
    }

    private SourceRecord createTransactionMetadataEvent(
            String status, String transactionId, long position) {
        Schema keySchema = SchemaBuilder.struct()
                .name(RecordUtils.SCHEMA_TRANSACTION_METADATA_EVENT_KEY_NAME)
                .field("id", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .name("io.debezium.connector.common.TransactionMetadataValue")
                .field("status", Schema.STRING_SCHEMA)
                .field("id", Schema.STRING_SCHEMA)
                .field("event_count", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();

        Struct key = new Struct(keySchema).put("id", transactionId);

        Struct value = new Struct(valueSchema)
                .put("status", status)
                .put("id", transactionId)
                .put("ts_ms", System.currentTimeMillis());

        if ("END".equals(status)) {
            value.put("event_count", 5L);
        }

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", position);
        offset.put("transaction_id", transactionId);

        return new SourceRecord(
                Collections.singletonMap("server", "mysql_binlog_source"),
                offset,
                "mysql_binlog_source.transaction",
                keySchema,
                key,
                valueSchema,
                value);
    }

    private SourceRecord createDataChangeEvent(String topicName, long position) {
        Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .field("op", Schema.STRING_SCHEMA)
                .field("after", SchemaBuilder.struct()
                        .field("id", Schema.INT32_SCHEMA)
                        .field("name", Schema.STRING_SCHEMA)
                        .optional())
                .build();

        Struct key = new Struct(keySchema).put("id", 1);
        Struct after = new Struct(valueSchema.field("after").schema())
                .put("id", 1)
                .put("name", "test");
        Struct value = new Struct(valueSchema)
                .put("op", "c")
                .put("after", after);

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", position);

        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset,
                topicName,
                keySchema,
                key,
                valueSchema,
                value);
    }

}
