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

package org.apache.flink.cdc.connectors.base.source.assigner.state;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;

import io.debezium.relational.TableId;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

/** Tests for {@link PendingSplitsStateSerializer}. */
public class PendingSplitsStateSerializerTest {

    private final TableId tableId = TableId.parse("catalog.schema.table1");

    @Test
    public void testOutputIsFinallyCleared() throws Exception {
        PendingSplitsStateSerializer serializer =
                new PendingSplitsStateSerializer(constructSourceSplitSerializer());
        StreamPendingSplitsState state = new StreamPendingSplitsState(true);

        final byte[] ser1 = serializer.serialize(state);
        state.serializedFormCache = null;

        PendingSplitsState unsupportedState = new UnsupportedPendingSplitsState();

        assertThrows(IOException.class, () -> serializer.serialize(unsupportedState));

        final byte[] ser2 = serializer.serialize(state);
        assertArrayEquals(ser1, ser2);
    }

    private SourceSplitSerializer constructSourceSplitSerializer() {
        return new SourceSplitSerializer() {
            @Override
            public OffsetFactory getOffsetFactory() {
                return new OffsetFactory() {
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
        };
    }

    /** An implementation for {@link PendingSplitsState} which will cause a serialization error. */
    static class UnsupportedPendingSplitsState extends PendingSplitsState {}
}
