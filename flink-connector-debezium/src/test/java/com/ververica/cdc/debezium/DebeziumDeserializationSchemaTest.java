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

import org.apache.flink.util.Collector;

import com.ververica.cdc.debezium.rate.GuavaRateLimiter;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

/** Unit tests for {@link DebeziumDeserializationSchema}. */
public class DebeziumDeserializationSchemaTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(DebeziumDeserializationSchemaTest.class);

    final Schema recordSchema =
            SchemaBuilder.struct()
                    .field("id", Schema.INT32_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build();

    final Schema sourceSchema =
            SchemaBuilder.struct()
                    .field("db", Schema.STRING_SCHEMA)
                    .field("table", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                    .build();

    final Envelope envelope =
            Envelope.defineSchema()
                    .withName("dummy.Envelope")
                    .withRecord(recordSchema)
                    .withSource(sourceSchema)
                    .build();

    @Test
    public void testDebeziumDeserializationSchemaWithRateLimit() throws Exception {
        List<SourceRecord> recordList = new ArrayList<>();

        int snapshotListSize = 10;
        for (int i = 1; i <= snapshotListSize; i++) {
            recordList.add(createCreateRecord(i, "myRecord_" + i, "myDb", "myTable", 0));
        }

        int binlogListSize = 10;
        for (int i = snapshotListSize + 1; i <= snapshotListSize + binlogListSize; i++) {
            recordList.add(
                    createCreateRecord(
                            i, "myRecord_" + i, "myDb", "myTable", Instant.now().getEpochSecond()));
        }

        int numParallelism = 2;
        long outputRateLimit = 2L;

        float snapshotExpectedSecond =
                snapshotListSize / ((float) outputRateLimit / numParallelism);
        float binlogExpectedSecond = (float) binlogListSize / outputRateLimit;
        // The first record at the beginning of the task is not included in the rate limit
        float expectedTime =
                (snapshotExpectedSecond + binlogExpectedSecond - numParallelism) * 1000;

        DebeziumDeserializationSchemaWithRateLimit<String> deserializationWithRateLimit =
                new DebeziumDeserializationSchemaWithRateLimit(
                        new StringDebeziumDeserializationSchema(),
                        new GuavaRateLimiter(outputRateLimit, numParallelism));

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (SourceRecord sourceRecord : recordList) {
            deserializationWithRateLimit.deserialize(sourceRecord, new BlackHoleCollector<>());
        }
        stopWatch.stop();

        assertTrue(stopWatch.getTime() > expectedTime);
    }

    private SourceRecord createCreateRecord(
            int id, String name, String db, String table, long timestamp) {
        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", id);
        before.put("name", name);
        source.put("db", db);
        source.put("table", table);
        source.put("ts_ms", timestamp);

        final Struct payload = envelope.create(before, source, Instant.now());
        return new SourceRecord(
                new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    private static class BlackHoleCollector<T> implements Collector<T> {

        @Override
        public void collect(T record) {}

        @Override
        public void close() {}
    }
}
