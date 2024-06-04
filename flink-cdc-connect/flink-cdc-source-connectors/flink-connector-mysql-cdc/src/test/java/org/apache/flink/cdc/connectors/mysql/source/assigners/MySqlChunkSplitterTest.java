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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlChunkSplitter}. */
class MySqlChunkSplitterTest {

    @Test
    void testSplitEvenlySizedChunksOverflow() {
        MySqlChunkSplitter splitter = new MySqlChunkSplitter(null, null);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 19,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        assertThat(res).hasSize(2);
        assertThat(res.get(0)).isEqualTo(ChunkRange.of(null, 2147483638));
        assertThat(res.get(1)).isEqualTo(ChunkRange.of(2147483638, null));
    }

    @Test
    void testSplitEvenlySizedChunksNormal() {
        MySqlChunkSplitter splitter = new MySqlChunkSplitter(null, null);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 20,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        assertThat(res).hasSize(3);
        assertThat(res.get(0)).isEqualTo(ChunkRange.of(null, 2147483637));
        assertThat(res.get(1)).isEqualTo(ChunkRange.of(2147483637, 2147483647));
        assertThat(res.get(2)).isEqualTo(ChunkRange.of(2147483647, null));
    }
}
