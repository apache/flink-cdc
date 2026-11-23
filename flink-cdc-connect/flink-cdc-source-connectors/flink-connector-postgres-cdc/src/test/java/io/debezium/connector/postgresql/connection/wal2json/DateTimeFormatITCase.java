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

package io.debezium.connector.postgresql.connection.wal2json;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs in a separate integration-test fork so the formatters initialize under a Chinese locale. */
class DateTimeFormatITCase {
    @Test
    void parsesEnglishEraUnderChineseLocale() {
        Locale.Category category = Locale.Category.FORMAT;
        Locale original = Locale.getDefault(category);
        try {
            Locale.setDefault(category, Locale.CHINA);
            DateTimeFormat parser = DateTimeFormat.get();
            LocalDateTime expected = LocalDateTime.of(0, 2, 29, 12, 34, 56, 123456000);
            assertThat(parser.date("0001-02-29 BC")).isEqualTo(expected.toLocalDate());
            assertThat(parser.timestampToInstant("0001-02-29 12:34:56.123456 BC"))
                    .isEqualTo(expected.toInstant(ZoneOffset.UTC));
            assertThat(
                            parser.timestampWithTimeZoneToOffsetDateTime(
                                    "0001-02-29 12:34:56.123456+08 BC"))
                    .isEqualTo(expected.atOffset(ZoneOffset.ofHours(8)));
            ZoneOffset historicalOffset = ZoneOffset.of("+08:05:43");
            assertThat(
                            parser.timestampWithTimeZoneToOffsetDateTime(
                                    "0001-02-29 12:34:56.123456+08:05:43 BC"))
                    .isEqualTo(expected.atOffset(historicalOffset));
            assertThat(parser.systemTimestampToInstant("0001-02-29 12:34:56.123456Z BC"))
                    .isEqualTo(expected.toInstant(ZoneOffset.UTC));
        } finally {
            Locale.setDefault(category, original);
        }
    }
}
