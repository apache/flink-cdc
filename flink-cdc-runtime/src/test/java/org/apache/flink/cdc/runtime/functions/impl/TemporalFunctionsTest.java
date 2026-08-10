/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.functions.impl;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TemporalFunctions}. */
class TemporalFunctionsTest {

    @Test
    void testExtractDateParts() {
        LocalDate date = LocalDate.of(2024, 2, 29);

        assertThat(TemporalFunctions.extract("YEAR", date, "UTC")).isEqualTo(2024L);
        assertThat(TemporalFunctions.extract("QUARTER", date, "UTC")).isEqualTo(1L);
        assertThat(TemporalFunctions.extract("MONTH", date, "UTC")).isEqualTo(2L);
        assertThat(TemporalFunctions.extract("WEEK", date, "UTC")).isEqualTo(9L);
        assertThat(TemporalFunctions.extract("DAY", date, "UTC")).isEqualTo(29L);
        assertThat(TemporalFunctions.extract("DOY", date, "UTC")).isEqualTo(60L);
        assertThat(TemporalFunctions.extract("DOW", date, "UTC")).isEqualTo(5L);
        assertThat(TemporalFunctions.extract("HOUR", date, "UTC")).isZero();
        assertThat(TemporalFunctions.extract("MINUTE", date, "UTC")).isZero();
        assertThat(TemporalFunctions.extract("SECOND", date, "UTC")).isZero();
    }

    @Test
    void testExtractTimeParts() {
        LocalTime time = LocalTime.of(23, 58, 57);

        assertThat(TemporalFunctions.extract("HOUR", time, "UTC")).isEqualTo(23L);
        assertThat(TemporalFunctions.extract("MINUTE", time, "UTC")).isEqualTo(58L);
        assertThat(TemporalFunctions.extract("SECOND", time, "UTC")).isEqualTo(57L);
    }

    @Test
    void testExtractTimestampParts() {
        LocalDateTime timestamp = LocalDateTime.of(2024, 12, 31, 23, 58, 57);

        assertThat(TemporalFunctions.extract("YEAR", timestamp, "UTC")).isEqualTo(2024L);
        assertThat(TemporalFunctions.extract("WEEK", timestamp, "UTC")).isEqualTo(1L);
        assertThat(TemporalFunctions.extract("HOUR", timestamp, "UTC")).isEqualTo(23L);
    }

    @Test
    void testExtractTimestampLtzUsesPipelineTimeZone() {
        Instant timestamp = Instant.parse("2023-12-31T16:30:00Z");

        assertThat(TemporalFunctions.extract("YEAR", timestamp, "UTC")).isEqualTo(2023L);
        assertThat(TemporalFunctions.extract("YEAR", timestamp, "Asia/Shanghai")).isEqualTo(2024L);
        assertThat(TemporalFunctions.extract("HOUR", timestamp, "Asia/Shanghai")).isEqualTo(0L);
    }

    @Test
    void testExtractTimestampTzUsesValueTimeZone() {
        assertThat(
                        TemporalFunctions.extract(
                                "HOUR",
                                Instant.parse("2024-01-01T00:00:00Z")
                                        .atZone(ZoneId.of("Asia/Shanghai")),
                                "UTC"))
                .isEqualTo(8L);
    }

    @Test
    void testExtractNullAndUnsupportedParts() {
        assertThat(TemporalFunctions.extract("YEAR", null, "UTC")).isNull();
        assertThatThrownBy(() -> TemporalFunctions.extract("YEAR", LocalTime.NOON, "UTC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("EXTRACT unit YEAR cannot be applied to LocalTime");
        assertThatThrownBy(
                        () -> TemporalFunctions.extract("MILLISECOND", LocalDateTime.now(), "UTC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported EXTRACT unit: MILLISECOND");
    }

    @Test
    void testTemporalPlusMonths() {
        assertThat(TemporalFunctions.temporalPlusMonths(LocalDate.of(2024, 1, 31), 1))
                .isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(
                        TemporalFunctions.temporalPlusMonths(
                                LocalDateTime.of(2024, 1, 31, 12, 34, 56), 13))
                .isEqualTo(LocalDateTime.of(2025, 2, 28, 12, 34, 56));
        assertThat(TemporalFunctions.temporalPlusMonths(Instant.parse("2024-01-31T12:34:56Z"), -1))
                .isEqualTo(Instant.parse("2023-12-31T12:34:56Z"));
        assertThat(TemporalFunctions.temporalPlusMonths(LocalTime.NOON, 12))
                .isEqualTo(LocalTime.NOON);
        assertThat(TemporalFunctions.temporalPlusMonths((LocalDate) null, 1)).isNull();
    }

    @Test
    void testTemporalPlusMillis() {
        assertThat(
                        TemporalFunctions.temporalPlusMillis(
                                LocalDate.of(2024, 2, 28), 24L * 60 * 60 * 1000))
                .isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(
                        TemporalFunctions.temporalPlusMillis(
                                LocalDate.of(2024, 2, 28), 12L * 60 * 60 * 1000))
                .isEqualTo(LocalDate.of(2024, 2, 28));
        assertThat(TemporalFunctions.temporalPlusMillis(LocalTime.of(23, 30), 2L * 60 * 60 * 1000))
                .isEqualTo(LocalTime.of(1, 30));
        assertThat(
                        TemporalFunctions.temporalPlusMillis(
                                LocalDateTime.of(2024, 2, 28, 23, 30), 2L * 60 * 60 * 1000))
                .isEqualTo(LocalDateTime.of(2024, 2, 29, 1, 30));
        assertThat(
                        TemporalFunctions.temporalPlusMillis(
                                Instant.parse("2024-02-28T23:30:00Z"), 2L * 60 * 60 * 1000))
                .isEqualTo(Instant.parse("2024-02-29T01:30:00Z"));
        assertThat(TemporalFunctions.temporalPlusMillis((Instant) null, 1)).isNull();
    }
}
