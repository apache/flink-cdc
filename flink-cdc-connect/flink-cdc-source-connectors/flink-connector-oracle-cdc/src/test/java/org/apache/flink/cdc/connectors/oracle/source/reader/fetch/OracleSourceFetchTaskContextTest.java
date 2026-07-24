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

package org.apache.flink.cdc.connectors.oracle.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for startup-offset resolution in {@link OracleSourceFetchTaskContext}. */
class OracleSourceFetchTaskContextTest {

    @Test
    void testDoNotResolveTimestampAgainWhenRestoredFromSpecificScnOffset() {
        StreamSplit restoredSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new RedoLogOffset(123L),
                        RedoLogOffset.NO_STOPPING_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        AtomicInteger resolveCalls = new AtomicInteger();

        OracleSourceFetchTaskContext.StartingOffsetResolution resolution =
                OracleSourceFetchTaskContext.resolveStartingOffset(
                        restoredSplit,
                        timestampMillis -> {
                            resolveCalls.incrementAndGet();
                            return new RedoLogOffset(999L);
                        });

        assertThat(resolveCalls.get()).isZero();
        assertThat(resolution.getStartupTimestampMillis()).isNull();
        assertThat(((RedoLogOffset) resolution.getOffset()).getScn()).isEqualTo("123");
    }

    @Test
    void testResolveTimestampOffsetForStreamSplitWhenTimestampMarkerExists() {
        long startupTimestampMillis = 1700000000000L;
        StreamSplit timestampSplit =
                new StreamSplit(
                        StreamSplit.STREAM_SPLIT_ID,
                        new RedoLogOffset(0L, 0L, null, startupTimestampMillis),
                        RedoLogOffset.NO_STOPPING_OFFSET,
                        new ArrayList<>(),
                        new HashMap<>(),
                        0);
        AtomicInteger resolveCalls = new AtomicInteger();
        AtomicLong observedTimestamp = new AtomicLong(-1L);

        OracleSourceFetchTaskContext.StartingOffsetResolution resolution =
                OracleSourceFetchTaskContext.resolveStartingOffset(
                        timestampSplit,
                        timestampMillis -> {
                            resolveCalls.incrementAndGet();
                            observedTimestamp.set(timestampMillis);
                            return new RedoLogOffset(456L);
                        });

        assertThat(resolveCalls.get()).isEqualTo(1);
        assertThat(observedTimestamp.get()).isEqualTo(startupTimestampMillis);
        assertThat(resolution.getStartupTimestampMillis()).isEqualTo(startupTimestampMillis);
        assertThat(((RedoLogOffset) resolution.getOffset()).getScn()).isEqualTo("456");
    }
}
