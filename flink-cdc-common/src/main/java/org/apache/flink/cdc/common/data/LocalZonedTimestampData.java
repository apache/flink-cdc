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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.utils.Preconditions;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * An internal data structure representing data of {@link LocalZonedTimestampType}.
 *
 * <p>This data structure is immutable and consists of a epoch milliseconds and epoch
 * nanos-of-millisecond since epoch {@code 1970-01-01 00:00:00}. It might be stored in a compact
 * representation (as a long value) if values are small enough.
 */
@PublicEvolving
public final class LocalZonedTimestampData implements Comparable<LocalZonedTimestampData> {

    // the number of milliseconds in a day
    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    // this field holds the epoch second and the milli-of-second
    private final long epochMillisecond;

    // this field holds the epoch nano-of-millisecond
    private final int epochNanoOfMillisecond;

    private LocalZonedTimestampData(long epochMillisecond, int epochNanoOfMillisecond) {
        Preconditions.checkArgument(
                epochNanoOfMillisecond >= 0 && epochNanoOfMillisecond <= 999_999);
        this.epochMillisecond = epochMillisecond;
        this.epochNanoOfMillisecond = epochNanoOfMillisecond;
    }

    /** Returns the number of epoch milliseconds since epoch {@code 1970-01-01 00:00:00}. */
    public long getEpochMillisecond() {
        return epochMillisecond;
    }

    /**
     * Returns the number of epoch nanoseconds (the nanoseconds within the milliseconds).
     *
     * <p>The value range is from 0 to 999,999.
     */
    public int getEpochNanoOfMillisecond() {
        return epochNanoOfMillisecond;
    }

    /** Converts this {@link LocalZonedTimestampData} object to a {@link Instant}. */
    public Instant toInstant() {
        long epochSecond = epochMillisecond / 1000;
        int milliOfSecond = (int) (epochMillisecond % 1000);
        if (milliOfSecond < 0) {
            --epochSecond;
            milliOfSecond += 1000;
        }
        long nanoAdjustment = milliOfSecond * 1_000_000 + epochNanoOfMillisecond;
        return Instant.ofEpochSecond(epochSecond, nanoAdjustment)
                .atZone(ZoneId.of("UTC"))
                .toInstant();
    }

    @Override
    public int compareTo(LocalZonedTimestampData that) {
        int cmp = Long.compare(this.epochMillisecond, that.epochMillisecond);
        if (cmp == 0) {
            cmp = this.epochNanoOfMillisecond - that.epochNanoOfMillisecond;
        }
        return cmp;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LocalZonedTimestampData)) {
            return false;
        }
        LocalZonedTimestampData that = (LocalZonedTimestampData) obj;
        return this.epochMillisecond == that.epochMillisecond
                && this.epochNanoOfMillisecond == that.epochNanoOfMillisecond;
    }

    @Override
    public int hashCode() {
        int ret = (int) epochMillisecond ^ (int) (epochMillisecond >> 32);
        return 31 * ret + epochNanoOfMillisecond;
    }

    @Override
    public String toString() {
        return describeLocalZonedTimestampInUTC0().toString();
    }

    /**
     * Describes this {@link LocalZonedTimestampData} object in {@link LocalDateTime} under UTC0
     * time zone.
     */
    private LocalDateTime describeLocalZonedTimestampInUTC0() {
        int date = (int) (epochMillisecond / MILLIS_PER_DAY);
        int time = (int) (epochMillisecond % MILLIS_PER_DAY);
        if (time < 0) {
            --date;
            time += MILLIS_PER_DAY;
        }
        long nanoOfDay = time * 1_000_000L + epochNanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    // ------------------------------------------------------------------------------------------
    // Constructor Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an instance of {@link LocalZonedTimestampData} from epoch milliseconds.
     *
     * <p>The nanos-of-millisecond field will be set to zero.
     *
     * @param millisecond the number of epoch milliseconds since epoch {@code 1970-01-01 00:00:00};
     *     a negative number is the number of epoch milliseconds before epoch {@code 1970-01-01
     *     00:00:00}
     */
    public static LocalZonedTimestampData fromEpochMillis(long millisecond) {
        return new LocalZonedTimestampData(millisecond, 0);
    }

    /**
     * Creates an instance of {@link LocalZonedTimestampData} from epoch milliseconds and a epoch
     * nanos-of-millisecond.
     *
     * @param millisecond the number of epoch milliseconds since epoch {@code 1970-01-01 00:00:00};
     *     a negative number is the number of epoch milliseconds before epoch {@code 1970-01-01
     *     00:00:00}
     * @param epochNanoOfMillisecond the epoch nanoseconds within the millisecond, from 0 to 999,999
     */
    public static LocalZonedTimestampData fromEpochMillis(
            long millisecond, int epochNanoOfMillisecond) {
        return new LocalZonedTimestampData(millisecond, epochNanoOfMillisecond);
    }

    /**
     * Creates an instance of {@link LocalZonedTimestampData} from an instance of {@link Instant}.
     *
     * @param instant an instance of {@link Instant}
     */
    public static LocalZonedTimestampData fromInstant(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nanoSecond = instant.getNano();

        long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
        int nanoOfMillisecond = nanoSecond % 1_000_000;

        return new LocalZonedTimestampData(millisecond, nanoOfMillisecond);
    }

    /**
     * Returns whether the local zoned timestamp data is small enough to be stored in a long of
     * milliseconds.
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }
}
