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

import java.time.LocalDate;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link
 * org.apache.flink.cdc.common.types.DateType}.
 */
public class DateData implements Comparable<DateData> {

    private final int epochDay;

    private DateData(int epochDay) {
        this.epochDay = epochDay;
    }

    public static DateData fromEpochDay(int epochOfDay) {
        return new DateData(epochOfDay);
    }

    public static DateData fromLocalDate(LocalDate date) {
        return fromEpochDay((int) date.toEpochDay());
    }

    public static DateData fromIsoLocalDateString(String dateString) {
        return fromLocalDate(LocalDate.parse(dateString));
    }

    public int toEpochDay() {
        return epochDay;
    }

    public LocalDate toLocalDate() {
        return LocalDate.ofEpochDay(epochDay);
    }

    public String toString() {
        return toLocalDate().toString();
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof DateData)) {
            return false;
        }

        DateData dateData = (DateData) o;
        return epochDay == dateData.epochDay;
    }

    @Override
    public int compareTo(DateData other) {
        return Long.compare(epochDay, other.epochDay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epochDay);
    }
}
