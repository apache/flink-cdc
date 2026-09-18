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

package org.apache.flink.cdc.connectors.tidb.source.split;

import org.apache.flink.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor.KeyRange;

import java.io.IOException;

/** Tests for {@link TiKVKeyRangeSplitSerializer}. */
class TiKVKeyRangeSplitSerializerTest {

    @Test
    void testRoundTrip() throws IOException {
        KeyRange range =
                KeyRangeUtils.makeCoprocRange(
                        RowKey.toRowKey(100L, 1L).toByteString(),
                        RowKey.toRowKey(100L, 200L).toByteString());
        TiKVKeyRangeSplit split = TiKVKeyRangeSplit.fromKeyRange("tidb-0", range, 42L);

        TiKVKeyRangeSplitSerializer serializer = TiKVKeyRangeSplitSerializer.INSTANCE;
        byte[] bytes = serializer.serialize(split);
        TiKVKeyRangeSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        Assertions.assertThat(restored).isEqualTo(split);
        Assertions.assertThat(restored.toKeyRange().getStart()).isEqualTo(range.getStart());
        Assertions.assertThat(restored.toKeyRange().getEnd()).isEqualTo(range.getEnd());
    }

    @Test
    void testEmptyRangeRoundTrip() throws IOException {
        KeyRange tableRange = TableKeyRangeUtils.getTableKeyRange(7L);
        byte[] start = tableRange.getStart().toByteArray();
        TiKVKeyRangeSplit split = new TiKVKeyRangeSplit("tidb-3", start, start, 0L);

        TiKVKeyRangeSplitSerializer serializer = TiKVKeyRangeSplitSerializer.INSTANCE;
        TiKVKeyRangeSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        Assertions.assertThat(restored.isEmpty()).isTrue();
        Assertions.assertThat(restored).isEqualTo(split);
    }
}
