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
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.tikv.common.meta.TiTimestamp;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that TiDB offsets are retained in Flink checkpoint state. */
class EventOffsetCheckpointTest {

    private static final long PHYSICAL_TIME_MILLIS = 1_782_896_898_607L;

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

    @Test
    void shouldKeepPackedTsoForOffsetAndUsePhysicalTimeForSourceInfo() {
        EventOffsetContext offsetContext =
                EventOffsetContext.initial(createSourceConfig().getDbzConnectorConfig());
        long commitVersion = new TiTimestamp(PHYSICAL_TIME_MILLIS, 123).getVersion();

        offsetContext.event(new TableId("inventory", null, "products"), commitVersion);

        assertThat(offsetContext.getOffset().get(EventOffset.TIMESTAMP_KEY))
                .isEqualTo(String.valueOf(PHYSICAL_TIME_MILLIS));
        assertThat(offsetContext.getOffset().get(EventOffset.COMMIT_VERSION_KEY))
                .isEqualTo(String.valueOf(commitVersion));
        Struct sourceInfo = offsetContext.getSourceInfo();
        assertThat(sourceInfo.getInt64(AbstractSourceInfo.TIMESTAMP_KEY))
                .isEqualTo(PHYSICAL_TIME_MILLIS);
        assertThat(sourceInfo.getInt64(TiDBSourceInfo.COMMIT_VERSION_KEY)).isEqualTo(commitVersion);
    }

    @Test
    void shouldNotCreateCheckpointTimestampWhenLoadedTimestampIsNull() {
        EventOffsetContext offsetContext =
                new EventOffsetContext.Loader(createSourceConfig().getDbzConnectorConfig())
                        .load(Collections.emptyMap());

        assertThat(offsetContext.getOffset())
                .doesNotContainKeys(EventOffset.TIMESTAMP_KEY, EventOffset.COMMIT_VERSION_KEY);
    }

    @Test
    void shouldKeepCommitVersionWhenLoadedTimestampIsNull() {
        String commitVersion = "467375724588433411";
        EventOffsetContext offsetContext =
                new EventOffsetContext.Loader(createSourceConfig().getDbzConnectorConfig())
                        .load(
                                Collections.singletonMap(
                                        EventOffset.COMMIT_VERSION_KEY, commitVersion));

        assertThat(offsetContext.getOffset()).doesNotContainKey(EventOffset.TIMESTAMP_KEY);
        assertThat(offsetContext.getOffset().get(EventOffset.COMMIT_VERSION_KEY))
                .isEqualTo(commitVersion);
    }

    private TiDBSourceConfig createSourceConfig() {
        TiDBSourceConfigFactory configFactory = new TiDBSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(4000);
        configFactory.username("root");
        configFactory.password("");
        configFactory.databaseList("inventory");
        configFactory.tableList("inventory.products");
        configFactory.pdAddresses("localhost:2379");
        return configFactory.create(0);
    }
}
