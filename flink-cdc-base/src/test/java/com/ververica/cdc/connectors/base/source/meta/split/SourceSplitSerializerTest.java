/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.base.source.meta.split;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SourceSplitSerializer}. */
public class SourceSplitSerializerTest {

    @Test
    public void testSnapshotTableIdSerializeAndDeserialize() throws IOException {
        SnapshotSplit snapshotSplitBefore =
                new SnapshotSplit(
                        new TableId("cata`log\"", "s\"che`ma", "ta\"ble.1`"),
                        "test",
                        new RowType(
                                Collections.singletonList(
                                        new RowType.RowField("id", new BigIntType()))),
                        null,
                        null,
                        null,
                        new HashMap<>());

        SourceSplitSerializer sourceSplitSerializer =
                new SourceSplitSerializer() {
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

        SnapshotSplit snapshotSplitAfter =
                (SnapshotSplit)
                        sourceSplitSerializer.deserialize(
                                sourceSplitSerializer.getVersion(),
                                sourceSplitSerializer.serialize(snapshotSplitBefore));

        assertEquals(snapshotSplitBefore.getTableId(), snapshotSplitAfter.getTableId());
    }
}
