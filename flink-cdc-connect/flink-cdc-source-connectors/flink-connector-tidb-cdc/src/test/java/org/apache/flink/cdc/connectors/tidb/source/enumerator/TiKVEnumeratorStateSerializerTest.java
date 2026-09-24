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

package org.apache.flink.cdc.connectors.tidb.source.enumerator;

import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/** Tests for {@link TiKVEnumeratorStateSerializer}. */
class TiKVEnumeratorStateSerializerTest {

    @Test
    void testRoundTrip() throws IOException {
        TiKVKeyRangeSplit split =
                new TiKVKeyRangeSplit("tidb-0", new byte[] {1, 2}, new byte[] {3, 4}, 99L);
        TiKVEnumeratorState state =
                new TiKVEnumeratorState(Collections.singletonList(split), true, 4);

        TiKVEnumeratorStateSerializer serializer = TiKVEnumeratorStateSerializer.INSTANCE;
        TiKVEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        Assertions.assertThat(restored).isEqualTo(state);
        Assertions.assertThat(restored.isEnumerated()).isTrue();
        Assertions.assertThat(restored.getParallelism()).isEqualTo(4);
        Assertions.assertThat(restored.getUnassignedSplits()).containsExactly(split);
    }

    @Test
    void testEmptyUnassigned() throws IOException {
        TiKVEnumeratorState state = new TiKVEnumeratorState(Arrays.asList(), true, 2);
        TiKVEnumeratorStateSerializer serializer = TiKVEnumeratorStateSerializer.INSTANCE;
        TiKVEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        Assertions.assertThat(restored.getUnassignedSplits()).isEmpty();
        Assertions.assertThat(restored.isEnumerated()).isTrue();
    }
}
