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

package org.apache.flink.cdc.connectors.fluss.source.split;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.StringType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussSplitSerializer}. */
class FlussSplitSerializerTest {

    private static final PhysicalTablePath PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("test_db", "test_table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1001L, 0);

    @Test
    void testSerializeLogSplitWithSchemaInfo() throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField("id", new IntType(false), null, 1),
                                new DataField("dt", new StringType(false), null, 2),
                                new DataField("name", new StringType(true), null, 3)));
        FlussLogSplit split =
                new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 100L, 1, rowType);

        FlussSplitSerializer serializer = new FlussSplitSerializer();
        FlussSplitBase restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(restored).isInstanceOf(FlussLogSplit.class);
        assertThat(restored.asLogSplit().getStartingOffset()).isEqualTo(100L);
        assertThat(restored.getSchemaId()).isEqualTo(1);
        assertThat(restored.getRowType()).isEqualTo(rowType);
    }

    @Test
    void testSerializeHybridSplit() throws Exception {
        FlussHybridSnapshotLogSplit split =
                new FlussHybridSnapshotLogSplit(
                        PHYSICAL_TABLE_PATH, TABLE_BUCKET, 10L, 2L, 20L, false, null, null);

        FlussSplitSerializer serializer = new FlussSplitSerializer();
        FlussSplitBase restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(restored).isInstanceOf(FlussHybridSnapshotLogSplit.class);
        FlussHybridSnapshotLogSplit restoredHybrid = restored.asHybridSnapshotLogSplit();
        assertThat(restoredHybrid.getSnapshotId()).isEqualTo(10L);
        assertThat(restoredHybrid.getRecordsToSkip()).isEqualTo(2L);
        assertThat(restoredHybrid.getLogStartingOffset()).isEqualTo(20L);
        assertThat(restoredHybrid.isSnapshotFinished()).isFalse();
    }

    @Test
    void testDeserializeUnknownSplitType() throws Exception {
        FlussSplitSerializer serializer = new FlussSplitSerializer();
        byte[] serialized =
                serializer.serialize(new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 100L));
        serialized[0] = 2;

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), serialized))
                .isInstanceOf(IOException.class)
                .hasMessage("Unknown split type: 2");
    }
}
