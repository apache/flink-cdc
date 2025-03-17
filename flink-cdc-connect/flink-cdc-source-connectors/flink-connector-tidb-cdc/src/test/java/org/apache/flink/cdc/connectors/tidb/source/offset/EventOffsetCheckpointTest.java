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

package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplitState;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that TiDB offsets are retained in Flink checkpoint state. */
class EventOffsetCheckpointTest {

    private final EventOffsetFactory offsetFactory = new EventOffsetFactory();
    private final SourceSplitSerializer splitSerializer =
            new SourceSplitSerializer() {
                @Override
                public OffsetFactory getOffsetFactory() {
                    return offsetFactory;
                }
            };

    @Test
    void shouldRestoreCheckpointedStreamOffset() throws Exception {
        StreamSplit streamSplit =
                new StreamSplit(
                        "stream-split",
                        EventOffset.INITIAL_OFFSET,
                        EventOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0);
        StreamSplitState streamSplitState = new StreamSplitState(streamSplit);
        EventOffset checkpointOffset = new EventOffset("1782896898607", "467375724588433411");

        streamSplitState.setStartingOffset(checkpointOffset);
        StreamSplit checkpointSplit = streamSplitState.toSourceSplit();

        byte[] checkpointBytes = splitSerializer.serialize(checkpointSplit);
        SourceSplitBase restored =
                splitSerializer.deserialize(splitSerializer.getVersion(), checkpointBytes);

        assertThat(restored.asStreamSplit().getStartingOffset()).isEqualTo(checkpointOffset);
    }
}
