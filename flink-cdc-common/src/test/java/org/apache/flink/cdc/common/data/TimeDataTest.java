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

package org.apache.flink.cdc.common.data;

import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimeData}. */
class TimeDataTest {

    @Test
    void preservesNanosecondsAcrossFactoriesAndConversions() {
        long nanos = 3_723_123_456_789L;
        TimeData time = TimeData.fromNanoOfDay(nanos);

        assertThat(time.toNanoOfDay()).isEqualTo(nanos);
        assertThat(time.toMicroOfDay()).isEqualTo(3_723_123_456L);
        assertThat(time.toMillisOfDay()).isEqualTo(3_723_123);
        assertThat(time.toLocalTime().toNanoOfDay()).isEqualTo(nanos);
        assertThat(TimeData.fromMicroOfDay(3_723_123_456L).toNanoOfDay())
                .isEqualTo(3_723_123_456_000L);
        assertThat(TimeData.fromLocalTime(LocalTime.of(1, 2, 3, 123_456_789)).toNanoOfDay())
                .isEqualTo(nanos);
    }

    @Test
    void comparisonAndEqualityIncludeSubMillisecondPrecision() {
        TimeData lower = TimeData.fromNanoOfDay(1_000_000_001L);
        TimeData higher = TimeData.fromNanoOfDay(1_000_000_999L);

        assertThat(lower).isNotEqualTo(higher);
        assertThat(lower.compareTo(higher)).isNegative();
        assertThat(lower.toMillisOfDay()).isEqualTo(higher.toMillisOfDay());
    }
}
