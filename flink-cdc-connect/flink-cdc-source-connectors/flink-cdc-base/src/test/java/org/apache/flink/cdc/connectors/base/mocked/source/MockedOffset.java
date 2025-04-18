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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/** An offset for mocked source. */
public class MockedOffset extends Offset {

    public static final MockedOffset MAX = new MockedOffset(Long.MAX_VALUE);
    private static final String OFFSET_INDEX_KEY = "offsetIndex";

    public MockedOffset(long offsetIndex) {
        this.offset = new HashMap<>();
        offset.put(OFFSET_INDEX_KEY, String.valueOf(offsetIndex));
    }

    public MockedOffset(Map<String, String> offset) {
        this.offset = new HashMap<>(offset);
    }

    public long getOffsetIndex() {
        return Long.parseLong(this.offset.get(OFFSET_INDEX_KEY));
    }

    @Override
    public int compareTo(@NotNull Offset that) {
        if (offset == null) {
            return -1;
        }
        return Long.compare(getOffsetIndex(), ((MockedOffset) that).getOffsetIndex());
    }
}
