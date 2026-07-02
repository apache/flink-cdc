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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IncrementalSourceReader}. */
class IncrementalSourceReaderTest {

    @Test
    void testInitializedStateAppliesSplitToRecordEmitter() {
        TestingReaderContext readerContext = new TestingReaderContext();
        TrackingRecordEmitter recordEmitter =
                new TrackingRecordEmitter(new SourceReaderMetrics(readerContext.metricGroup()));
        TestingIncrementalSourceReader reader =
                new TestingIncrementalSourceReader(
                        recordEmitter, new IncrementalSourceReaderContext(readerContext));

        SnapshotSplit split =
                new SnapshotSplit(
                        new TableId("catalog", "schema", "table"),
                        "split-0",
                        RowType.of(new IntType()),
                        null,
                        null,
                        null,
                        Collections.emptyMap());

        reader.initialize(split);

        assertThat(recordEmitter.appliedSplit).isSameAs(split);
    }

    private static class TestingIncrementalSourceReader
            extends IncrementalSourceReader<Object, SourceConfig> {

        private TestingIncrementalSourceReader(
                TrackingRecordEmitter recordEmitter,
                IncrementalSourceReaderContext incrementalSourceReaderContext) {
            super(
                    new FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>>(),
                    createSplitReaderSupplier(),
                    recordEmitter,
                    new Configuration(),
                    incrementalSourceReaderContext,
                    null,
                    new SourceSplitSerializer() {
                        @Override
                        public OffsetFactory getOffsetFactory() {
                            return TEST_OFFSET_FACTORY;
                        }
                    },
                    null);
        }

        private static Supplier<IncrementalSourceSplitReader<SourceConfig>>
                createSplitReaderSupplier() {
            return () -> null;
        }

        private SourceSplitState initialize(SourceSplitBase split) {
            return initializedState(split);
        }
    }

    private static class TrackingRecordEmitter extends IncrementalSourceRecordEmitter<Object> {

        private SourceSplitBase appliedSplit;

        private TrackingRecordEmitter(SourceReaderMetrics sourceReaderMetrics) {
            super(
                    new NoOpDebeziumDeserializationSchema(),
                    sourceReaderMetrics,
                    false,
                    TEST_OFFSET_FACTORY);
        }

        @Override
        public void applySplit(SourceSplitBase split) {
            this.appliedSplit = split;
        }
    }

    private static class NoOpDebeziumDeserializationSchema
            implements DebeziumDeserializationSchema<Object> {

        @Override
        public void deserialize(SourceRecord record, Collector<Object> out) {
            // no-op
        }

        @Override
        public TypeInformation<Object> getProducedType() {
            return TypeInformation.of(Object.class);
        }
    }

    private static final OffsetFactory TEST_OFFSET_FACTORY =
            new OffsetFactory() {
                @Override
                public Offset newOffset(Map<String, String> offset) {
                    return null;
                }

                @Override
                public Offset newOffset(String filename, Long position) {
                    return null;
                }

                @Override
                public Offset newOffset(Long position) {
                    return null;
                }

                @Override
                public Offset createTimestampOffset(long timestampMillis) {
                    return null;
                }

                @Override
                public Offset createInitialOffset() {
                    return null;
                }

                @Override
                public Offset createNoStoppingOffset() {
                    return null;
                }
            };
}
