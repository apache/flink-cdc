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

    private static final int SECONDS_TO_MILLIS = 1000;
    private static final int MILLIS_TO_MICRO = 1000;
    private static final int MILLIS_TO_NANO = 1_000_000;

    private final int millisOfDay;

    private TimeData(int millisOfDay) {
        this.millisOfDay = millisOfDay;
    }

    public static TimeData fromSecondOfDay(int secondOfDay) {
        return new TimeData(secondOfDay * SECONDS_TO_MILLIS);
    }

    public static TimeData fromMillisOfDay(int millisOfDay) {
        return new TimeData(millisOfDay);
    }

    public static TimeData fromMicroOfDay(long microOfDay) {
        return new TimeData((int) (microOfDay / MILLIS_TO_MICRO));
    }

    public static TimeData fromNanoOfDay(long nanoOfDay) {
        // millisOfDay should not exceed 86400000, which is safe to fit into INT.
        return new TimeData((int) (nanoOfDay / MILLIS_TO_NANO));
    }

    public static TimeData fromLocalTime(LocalTime localTime) {
        return fromNanoOfDay(localTime.toNanoOfDay());
    }

    public static TimeData fromIsoLocalTimeString(String timeString) {
        return fromLocalTime(LocalTime.parse(timeString));
    }

    public int toMillisOfDay() {
        return millisOfDay;
    }

    public LocalTime toLocalTime() {
        return LocalTime.ofNanoOfDay((long) millisOfDay * MILLIS_TO_NANO);
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
        return millisOfDay == timeData.millisOfDay;
    }

    @Override
    public int compareTo(TimeData other) {
        return Long.compare(millisOfDay, other.millisOfDay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(millisOfDay);
    }
}
