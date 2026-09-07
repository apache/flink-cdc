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

import java.time.LocalTime;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link
 * org.apache.flink.cdc.common.types.TimeType}.
 */
public class TimeData implements Comparable<TimeData> {

    private static final long SECONDS_TO_NANO = 1_000_000_000L;
    private static final long MILLIS_TO_NANO = 1_000_000L;
    private static final long MICRO_TO_NANO = 1_000L;

    private final long nanoOfDay;

    private TimeData(long nanoOfDay) {
        this.nanoOfDay = nanoOfDay;
    }

    public static TimeData fromSecondOfDay(int secondOfDay) {
        return new TimeData(secondOfDay * SECONDS_TO_NANO);
    }

    public static TimeData fromMillisOfDay(int millisOfDay) {
        return new TimeData(millisOfDay * MILLIS_TO_NANO);
    }

    public static TimeData fromMicroOfDay(long microOfDay) {
        return new TimeData(microOfDay * MICRO_TO_NANO);
    }

    public static TimeData fromNanoOfDay(long nanoOfDay) {
        return new TimeData(nanoOfDay);
    }

    public static TimeData fromLocalTime(LocalTime localTime) {
        return fromNanoOfDay(localTime.toNanoOfDay());
    }

    public static TimeData fromIsoLocalTimeString(String timeString) {
        return fromLocalTime(LocalTime.parse(timeString));
    }

    public int toMillisOfDay() {
        return (int) (nanoOfDay / MILLIS_TO_NANO);
    }

    public long toMicroOfDay() {
        return nanoOfDay / MICRO_TO_NANO;
    }

    public long toNanoOfDay() {
        return nanoOfDay;
    }

    public LocalTime toLocalTime() {
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    public String toString() {
        return toLocalTime().toString();
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof TimeData)) {
            return false;
        }

        TimeData timeData = (TimeData) o;
        return nanoOfDay == timeData.nanoOfDay;
    }

    @Override
    public int compareTo(TimeData other) {
        return Long.compare(nanoOfDay, other.nanoOfDay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nanoOfDay);
    }
}
