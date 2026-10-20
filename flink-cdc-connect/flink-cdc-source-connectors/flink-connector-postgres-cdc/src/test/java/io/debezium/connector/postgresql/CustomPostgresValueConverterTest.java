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

package io.debezium.connector.postgresql;

import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.config.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

class CustomPostgresValueConverterTest {

    private static TimeZone originalTimeZone;

    private final CustomPostgresValueConverter wallClockConverter = customConverter(true);
    private final CustomPostgresValueConverter defaultConverter = customConverter(false);
    private final PostgresValueConverter debeziumConverter = debeziumConverter();

    @BeforeAll
    static void useRegionalTimeZone() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    @AfterAll
    static void restoreTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    void testPreEpochTimestampKeepsWallClockWhenEnabled() {
        assertThat(convert(wallClockConverter, "1600-01-01 00:00:00.123456"))
                .isEqualTo(LocalDateTime.parse("1600-01-01T00:00:00.123456"));
        assertThat(convert(wallClockConverter, "1900-01-01 00:00:00.123456"))
                .isEqualTo(LocalDateTime.parse("1900-01-01T00:00:00.123456"));
    }

    @Test
    void testTimestampAroundEpochKeepsWallClockWhenEnabled() {
        assertThat(convert(wallClockConverter, "1969-12-31 23:59:59.999999"))
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999"));
        assertThat(convert(wallClockConverter, "1970-01-01 00:00:00"))
                .isEqualTo(LocalDateTime.parse("1970-01-01T00:00:00"));
        assertThat(convert(wallClockConverter, "2020-07-17 18:00:22.123456"))
                .isEqualTo(LocalDateTime.parse("2020-07-17T18:00:22.123456"));
    }

    @Test
    void testDefaultKeepsDebeziumConversion() {
        for (String value :
                new String[] {
                    "1600-01-01 00:00:00.123456",
                    "1900-01-01 00:00:00.123456",
                    "1969-12-31 23:59:59.999999",
                    "1970-01-01 00:00:00",
                    "2020-07-17 18:00:22.123456",
                }) {
            assertThat(convert(defaultConverter, value))
                    .isEqualTo(convert(debeziumConverter, value));
        }
    }

    @Test
    void testPreEpochTimestampIsShiftedWhenDisabled() {
        // Asia/Shanghai was UTC+08:05:43 before 1901, which makes the conversion of Debezium move
        // the date and time fields of timestamps before 1970-01-01.
        LocalDateTime converted =
                (LocalDateTime) convert(defaultConverter, "1900-01-01 00:00:00.123456");
        assertThat(converted).isNotEqualTo(LocalDateTime.parse("1900-01-01T00:00:00.123456"));
        assertThat(convert(wallClockConverter, "1900-01-01 00:00:00.123456"))
                .isEqualTo(LocalDateTime.parse("1900-01-01T00:00:00.123456"));
    }

    @Test
    void testNullAndNonTimestampValues() {
        for (CustomPostgresValueConverter converter :
                new CustomPostgresValueConverter[] {defaultConverter, wallClockConverter}) {
            assertThat(converter.convertTimestampToLocalDateTime(null, null, null)).isNull();

            Object value = "1900-01-01 00:00:00";
            assertThat(converter.convertTimestampToLocalDateTime(null, null, value))
                    .isSameAs(value);
        }
    }

    @Test
    void testInfinityTimestamps() {
        for (CustomPostgresValueConverter converter :
                new CustomPostgresValueConverter[] {defaultConverter, wallClockConverter}) {
            assertThat(
                            converter.convertTimestampToLocalDateTime(
                                    null, null, PostgresValueConverter.POSITIVE_INFINITY_TIMESTAMP))
                    .isEqualTo(PostgresValueConverter.POSITIVE_INFINITY_LOCAL_DATE_TIME);
            assertThat(
                            converter.convertTimestampToLocalDateTime(
                                    null, null, PostgresValueConverter.NEGATIVE_INFINITY_TIMESTAMP))
                    .isEqualTo(PostgresValueConverter.NEGATIVE_INFINITY_LOCAL_DATE_TIME);
        }
    }

    private static Object convert(PostgresValueConverter converter, String value) {
        return converter.convertTimestampToLocalDateTime(null, null, Timestamp.valueOf(value));
    }

    /**
     * Tests that the option also reaches the value converter when it is passed as a Debezium
     * property, which is how the DataStream API and the Postgres pipeline connector enable it.
     */
    @Test
    void testOptionReadFromDebeziumPropertiesOfSourceConfig() {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty(
                PostgresSourceOptions.SCAN_PRE_EPOCH_TIMESTAMP_WALL_CLOCK_CONVERSION_ENABLED.key(),
                "true");

        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.username("postgres");
        configFactory.password("postgres");
        configFactory.database("postgres");
        configFactory.schemaList(new String[] {"inventory"});
        configFactory.tableList("inventory.full_types");
        configFactory.decodingPluginName("pgoutput");
        configFactory.debeziumProperties(debeziumProperties);

        PostgresSourceConfig sourceConfig = configFactory.create(0);

        CustomPostgresValueConverter converter =
                CustomPostgresValueConverter.of(
                        sourceConfig.getDbzConnectorConfig(), StandardCharsets.UTF_8, null);
        assertThat(convert(converter, "1900-01-01 00:00:00.123"))
                .isEqualTo(LocalDateTime.parse("1900-01-01T00:00:00.123"));
    }

    private static CustomPostgresValueConverter customConverter(boolean optionEnabled) {
        Configuration configuration =
                TestHelper.defaultConfig()
                        .with(
                                PostgresSourceOptions
                                        .SCAN_PRE_EPOCH_TIMESTAMP_WALL_CLOCK_CONVERSION_ENABLED
                                        .key(),
                                String.valueOf(optionEnabled))
                        .build();
        return CustomPostgresValueConverter.of(
                new PostgresConnectorConfig(configuration), StandardCharsets.UTF_8, null);
    }

    private static PostgresValueConverter debeziumConverter() {
        return PostgresValueConverter.of(
                new PostgresConnectorConfig(TestHelper.defaultConfig().build()),
                StandardCharsets.UTF_8,
                null);
    }
}
