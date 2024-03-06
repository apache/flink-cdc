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
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.common.utils.Preconditions;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
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

    // the number of milliseconds in a day
    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    // this field holds the integral second and the milli-of-second
    private final long millisecond;

    // this field holds the nano-of-millisecond
    private final int nanoOfMillisecond;

    // this field holds time zone id
    private final String zoneId;

    private ZonedTimestampData(long millisecond, int nanoOfMillisecond, String zoneId) {
        Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
        Preconditions.checkNotNull(zoneId);
        this.millisecond = millisecond;
        this.nanoOfMillisecond = nanoOfMillisecond;
        this.zoneId = zoneId;
    }

    /** Returns the zoned date-time with time-zone. */
    public ZonedDateTime getZonedDateTime() {
        return ZonedDateTime.of(getLocalDateTimePart(), ZoneId.of(zoneId));
    }

    /** Converts this {@link ZonedTimestampData} object to a {@link Instant}. */
    public Instant toInstant() {
        return ZonedDateTime.of(getLocalDateTimePart(), ZoneId.of(zoneId)).toInstant();
    }

    /** Converts this {@link ZonedTimestampData} object to a {@link Timestamp}. */
    public Timestamp toTimestamp() {
        return Timestamp.from(toInstant());
    }

    /** Returns the number of milliseconds since {@code 1970-01-01 00:00:00}. */
    public long getMillisecond() {
        return millisecond;
    }

    /**
     * Returns the number of nanoseconds (the nanoseconds within the milliseconds).
     *
     * <p>The value range is from 0 to 999,999.
     */
    public int getNanoOfMillisecond() {
        return nanoOfMillisecond;
    }

    /** Returns the {@code LocalDateTime} part of this zoned date-time. */
    public LocalDateTime getLocalDateTimePart() {
        int date = (int) (millisecond / MILLIS_PER_DAY);
        int time = (int) (millisecond % MILLIS_PER_DAY);
        if (time < 0) {
            --date;
            time += MILLIS_PER_DAY;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    /** Returns the {@code ZoneId} part of this zoned date-time. */
    public ZoneId getTimeZoneId() {
        return ZoneId.of(zoneId);
    }

    /** Returns the {@code ZoneId} string of this zoned date-time. */
    public String getZoneId() {
        return zoneId;
    }

    @Override
    public int compareTo(ZonedTimestampData that) {
        // converts to instant and then compare
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
        return this.millisecond == that.millisecond
                && this.nanoOfMillisecond == that.nanoOfMillisecond
                && this.zoneId.equals(that.zoneId);
    }

    @Override
    public String toString() {
        return getZonedDateTime().format(ISO_FORMATTER);
    }

    @Override
    public int hashCode() {
        return Objects.hash(millisecond, nanoOfMillisecond, zoneId);
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
        LocalDateTime dateTimePart = zonedDateTime.toLocalDateTime();
        long epochDay = dateTimePart.toLocalDate().toEpochDay();
        long nanoOfDay = dateTimePart.toLocalTime().toNanoOfDay();
        long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
        int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);
        String zoneId = zonedDateTime.getZone().toString();

        return new ZonedTimestampData(millisecond, nanoOfMillisecond, zoneId);
    }

    /**
     * Creates an instance of {@link ZonedTimestampData} from an instance of {@link LocalDateTime}.
     *
     * @param offsetDateTime an instance of {@link OffsetDateTime}
     */
    public static ZonedTimestampData fromOffsetDateTime(OffsetDateTime offsetDateTime) {
        return fromZonedDateTime(offsetDateTime.toZonedDateTime());
    }

    /**
     * Creates an instance of {@link ZonedTimestampData} from milliseconds and nanos-of-millisecond
     * with the given zoneId.
     *
     * @param millisecond the number of milliseconds
     * @param nanoOfMillisecond the nanoseconds
     * @param zoneId the zoneId string
     */
    public static ZonedTimestampData of(long millisecond, int nanoOfMillisecond, String zoneId) {
        return new ZonedTimestampData(millisecond, nanoOfMillisecond, zoneId);
    }

    /**
     * Returns whether the date-time part is small enough to be stored in a long of milliseconds.
     */
    public static boolean isCompact(int precision) {
        return precision <= 3;
    }
}
