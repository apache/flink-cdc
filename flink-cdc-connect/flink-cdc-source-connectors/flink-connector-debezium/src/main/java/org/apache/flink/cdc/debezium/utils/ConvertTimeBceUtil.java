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

package org.apache.flink.cdc.debezium.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.chrono.IsoEra;

/** Convert And Check TimeBce Util. */
public class ConvertTimeBceUtil {

    private static final Date ONE_CE = Date.valueOf("0001-01-01");

    public static String resolveEra(boolean isBce, String value) {
        String mangledValue = value;
        if (isBce) {
            if (mangledValue.startsWith("-")) {
                mangledValue = mangledValue.substring(1);
            }
            if (!mangledValue.endsWith(" BC")) {
                mangledValue += " BC";
            }
        }
        return mangledValue;
    }

    public static boolean isBce(LocalDate date) {
        return date.getEra() == IsoEra.BCE;
    }

    public static String resolveEra(LocalDate date, String value) {
        return resolveEra(isBce(date), value);
    }

    public static String resolveEra(Date date, String value) {
        return resolveEra(date.before(ONE_CE), value);
    }

    public static String resolveEra(Timestamp timestamp, String value) {
        return resolveEra(timestamp.before(ONE_CE), value);
    }
}
