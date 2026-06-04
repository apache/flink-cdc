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

package org.apache.flink.cdc.connectors.oracle.source.meta.offset;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RedoLogOffset}. */
class RedoLogOffsetTest {

    @Test
    void testCompareByLcrPositionWhenScnIsAbsent() {
        RedoLogOffset earlier = lcrOffset("0000000008352fb60000000100000001");
        RedoLogOffset later = lcrOffset("0000000008363a3a0000000100000001");

        assertThat(later).isGreaterThan(earlier);
        assertThat(earlier).isLessThan(later);
    }

    @Test
    void testNullStringLcrPositionIsNotTreatedAsProgress() {
        RedoLogOffset empty = lcrOffset("null");
        RedoLogOffset later = lcrOffset("0000000008363a3a0000000100000001");

        assertThat(later).isGreaterThan(empty);
        assertThat(empty).isLessThan(later);
    }

    private static RedoLogOffset lcrOffset(String lcrPosition) {
        Map<String, String> offset = new HashMap<>();
        offset.put(RedoLogOffset.LCR_POSITION_KEY, lcrPosition);
        offset.put("snapshot_scn", "null");
        offset.put("transaction_id", "null");
        return new RedoLogOffset(offset);
    }
}
