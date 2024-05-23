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

package org.apache.flink.cdc.connectors.base.source.meta.split;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.version4.LegacySourceSplitSerializierVersion4;
import org.apache.flink.cdc.connectors.base.source.meta.split.version4.StreamSplitVersion4;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SourceSplitSerializer}. */
public class SourceSplitSerializerTest {

    @Test
    public void testSourceSplitSerializeAndDeserialize() throws IOException {
        SnapshotSplit snapshotSplitBefore = constuctSnapshotSplit();
        SourceSplitSerializer sourceSplitSerializer = constructSourceSplitSerializer();
        SnapshotSplit snapshotSplitAfter =
                (SnapshotSplit)
                        sourceSplitSerializer.deserialize(
                                sourceSplitSerializer.getVersion(),
                                sourceSplitSerializer.serialize(snapshotSplitBefore));

        assertEquals(snapshotSplitBefore, snapshotSplitAfter);
        assertEquals(snapshotSplitBefore.getTableSchemas(), snapshotSplitAfter.getTableSchemas());

        StreamSplit streamSplitBefore = constuctStreamSplit(true);
        StreamSplit streamSplitAfter =
                (StreamSplit)
                        sourceSplitSerializer.deserialize(
                                sourceSplitSerializer.getVersion(),
                                sourceSplitSerializer.serialize(streamSplitBefore));

        assertEquals(streamSplitBefore, streamSplitAfter);
    }

    @Test
    public void testStreamSplitBackwardCompatibility() throws IOException {
        SnapshotSplit snapshotSplitBefore = constuctSnapshotSplit();
        SourceSplitSerializer sourceSplitSerializer = constructSourceSplitSerializer();
        SnapshotSplit snapshotSplitAfter =
                (SnapshotSplit)
                        sourceSplitSerializer.deserialize(
                                4,
                                LegacySourceSplitSerializierVersion4.serialize(
                                        snapshotSplitBefore));

        assertEquals(snapshotSplitBefore, snapshotSplitAfter);

        StreamSplitVersion4 streamSplitVersion4Before = constuctStreamSplitVersion4();
        StreamSplit expectedStreamSplit = constuctStreamSplit(false);
        StreamSplit streamSplitAfter =
                (StreamSplit)
                        sourceSplitSerializer.deserialize(
                                4,
                                LegacySourceSplitSerializierVersion4.serialize(
                                        streamSplitVersion4Before));

        assertEquals(expectedStreamSplit, streamSplitAfter);
    }

    private SnapshotSplit constuctSnapshotSplit() {
        TableId tableId = new TableId("cata`log\"", "s\"che`ma", "ta\"ble.1`");
        HashMap<TableId, TableChanges.TableChange> map = new HashMap<>();
        Tables tables = new Tables();
        Table table = tables.editOrCreateTable(tableId).create();
        TableChanges.TableChange tableChange =
                new TableChanges.TableChange(TableChanges.TableChangeType.CREATE, table);
        map.put(tableId, tableChange);
        return new SnapshotSplit(
                tableId,
                "test",
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType()))),
                null,
                null,
                null,
                map);
    }

    private StreamSplit constuctStreamSplit(boolean isSuspend) {
        return new StreamSplit(
                "database1.schema1.table1",
                null,
                null,
                new ArrayList<>(),
                new HashMap<>(),
                0,
                isSuspend);
    }

    private StreamSplitVersion4 constuctStreamSplitVersion4() {
        return new StreamSplitVersion4(
                "database1.schema1.table1", null, null, new ArrayList<>(), new HashMap<>(), 0);
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
}
