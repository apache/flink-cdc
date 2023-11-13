/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.common.data;

import org.apache.flink.annotation.PublicEvolving;

import com.ververica.cdc.common.types.ZonedTimestampType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link ZonedTimestampType}. It aims to converting
 * various Java time representations into the date-time in a particular time zone.
 *
 * <p>The ISO date-time format is used by default, it includes the date, time (including fractional
 * parts), and offset from UTC, such as '2011-12-03T10:15:30+01:00'.
 */
@PublicEvolving
public final class ZonedTimestampData implements Comparable<ZonedTimestampData> {

    /**
     * The ISO date-time format includes the date, time (including fractional parts), and offset
     * from UTC, such as '2011-12-03T10:15:30.030431+01:00'.
     */
    public static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final ZonedDateTime zonedDateTime;

    private ZonedTimestampData(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
    }

    /** Returns the zoned date-time with time-zone. */
    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    /** Converts this {@link ZonedTimestampData} object to a {@link Timestamp}. */
    public Timestamp toTimestamp() {
        return Timestamp.from(zonedDateTime.toInstant());
    }

    /** Converts this {@link ZonedTimestampData} object to a {@link Instant}. */
    public Instant toInstant() {
        return zonedDateTime.toInstant();
    }

    @Override
    public int compareTo(ZonedTimestampData that) {
        long epochMillisecond = this.toInstant().getEpochSecond();
        int epochNanoOfMillisecond = this.toInstant().getNano();
        long thatEpochMillisecond = that.toInstant().getEpochSecond();
        int thatEpochNanoOfMillisecond = that.toInstant().getNano();

        int cmp = Long.compare(epochMillisecond, thatEpochMillisecond);
        if (cmp == 0) {
            cmp = epochNanoOfMillisecond - thatEpochNanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ZonedTimestampData)) {
            return false;
        }
        ZonedTimestampData that = (ZonedTimestampData) obj;
        return this.zonedDateTime.equals(that.zonedDateTime);
    }

    @Override
    public String toString() {
        return zonedDateTime.format(ISO_FORMATTER);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zonedDateTime);
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link ZonedTimestampData} from an instance of {@link LocalDateTime}.
     *
     * @param zonedDateTime an instance of {@link ZonedDateTime}
     */
    public static ZonedTimestampData fromZonedDateTime(ZonedDateTime zonedDateTime) {
        return new ZonedTimestampData(zonedDateTime);
    }

    /**
     * Creates an instance of {@link ZonedTimestampData} from an instance of {@link LocalDateTime}.
     *
     * @param offsetDateTime an instance of {@link OffsetDateTime}
     */
    public static ZonedTimestampData fromOffsetDateTime(OffsetDateTime offsetDateTime) {
        return new ZonedTimestampData(offsetDateTime.toZonedDateTime());
    }

    /**
     * Creates an instance of {@link ZonedTimestampData} from epoch milliseconds.
     *
     * <p>The epoch nanos-of-millisecond field will be set to zero.
     *
     * @param epochMillisecond the number of epoch milliseconds since epoch {@code 1970-01-01
     *     00:00:00}; a negative number is the number of epoch milliseconds before epoch {@code
     *     1970-01-01 00:00:00}
     */
    public static ZonedTimestampData fromEpochMillis(long epochMillisecond) {
        return fromEpochMillis(epochMillisecond, 0);
    }

    /**
     * Creates an instance of {@link ZonedTimestampData} from epoch milliseconds and a epoch
     * nanos-of-millisecond.
     *
     * @param epochMillisecond the number of epoch milliseconds since epoch {@code 1970-01-01
     *     00:00:00}; a negative number is the number of epoch milliseconds before epoch {@code
     *     1970-01-01 00:00:00}
     * @param epochNanoOfMillisecond the nanoseconds within the epoch millisecond, from 0 to 999,999
     */
    public static ZonedTimestampData fromEpochMillis(
            long epochMillisecond, int epochNanoOfMillisecond) {
        long epochSecond = epochMillisecond / 1000;
        int milliOfSecond = (int) (epochMillisecond % 1000);
        if (milliOfSecond < 0) {
            --epochSecond;
            milliOfSecond += 1000;
        }
        long nanoAdjustment = milliOfSecond * 1_000_000 + epochNanoOfMillisecond;
        return fromInstant(Instant.ofEpochSecond(epochSecond, nanoAdjustment));
    }

    /**
     * Creates an instance of {@link ZonedTimestampData} from an instance of {@link Instant}.
     *
     * @param instant an instance of {@link Instant}
     */
    public static ZonedTimestampData fromInstant(Instant instant) {
        return new ZonedTimestampData(ZonedDateTime.from(instant));
    }
}
