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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link ZonedTimeType}. It aims to converting
 * various Java time representations into the date-time in a particular time zone.
 *
 * <p>The ISO time format is used by default, it includes the date, time (including fractional
 * parts), and offset from UTC, such as '10:15:30+01:00'.
 */
@PublicEvolving
public class ZoneTimeData implements Comparable<ZoneTimeData> {

    private static final long MILLIS_TO_NANO = 1_000L;
    /**
     * The ISO time format includes the date, time (including fractional parts), and offset from
     * UTC, such as '10:15:30.030431+01:00'.
     */
    public static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_TIME;

    private final long nanoOfDay;
    // this field holds time zone id
    private final String zoneId;

    public ZoneTimeData(long nanoOfDay, String zoneId) {
        this.nanoOfDay = nanoOfDay;
        this.zoneId = zoneId;
    }

    public static ZoneTimeData fromLocalTime(LocalTime localTime, ZoneId zoneId) {
        return new ZoneTimeData(localTime.toNanoOfDay(), zoneId.getId());
    }

    public static ZoneTimeData fromNanoOfDay(long nanoOfDay, ZoneId zoneId) {
        return new ZoneTimeData(nanoOfDay, zoneId.getId());
    }

    public static ZoneTimeData fromMillsOfDay(long millsOfDay, ZoneId zoneId) {
        return new ZoneTimeData(millsOfDay * MILLIS_TO_NANO, zoneId.getId());
    }

    public static ZoneTimeData fromIsoLocalTimeString(String timeString, ZoneId zoneId) {
        return fromLocalTime(LocalTime.parse(timeString), zoneId);
    }

    public ZonedDateTime toZoneLocalTime() {
        return ZonedDateTime.ofInstant(
                Instant.from(LocalDateTime.of(LocalDate.now(), LocalTime.ofNanoOfDay(nanoOfDay))),
                ZoneId.of(zoneId));
    }

    public String toString() {
        return toZoneLocalTime().format(ISO_FORMATTER);
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof ZoneTimeData)) {
            return false;
        }

        ZoneTimeData timeData = (ZoneTimeData) o;
        return nanoOfDay == timeData.nanoOfDay;
    }

    @Override
    public int compareTo(ZoneTimeData other) {
        return Long.compare(nanoOfDay, other.nanoOfDay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nanoOfDay, zoneId);
    }
}
