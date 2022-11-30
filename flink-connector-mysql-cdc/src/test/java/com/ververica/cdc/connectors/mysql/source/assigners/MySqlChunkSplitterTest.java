/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.assigners;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link MySqlChunkSplitter}. */
public class MySqlChunkSplitterTest {

    @Test
    public void testSplitEvenlySizedChunksOverflow() {
        MySqlChunkSplitter splitter = new MySqlChunkSplitter(null, null);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 19,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        assertEquals(2, res.size());
        assertEquals(ChunkRange.of(null, 2147483638), res.get(0));
        assertEquals(ChunkRange.of(2147483638, null), res.get(1));
    }

    @Test
    public void testSplitEvenlySizedChunksNormal() {
        MySqlChunkSplitter splitter = new MySqlChunkSplitter(null, null);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 20,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        assertEquals(3, res.size());
        assertEquals(ChunkRange.of(null, 2147483637), res.get(0));
        assertEquals(ChunkRange.of(2147483637, 2147483647), res.get(1));
        assertEquals(ChunkRange.of(2147483647, null), res.get(2));
    }
}
