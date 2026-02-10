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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.table.types.logical.DecimalType;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresTypeUtils}. */
class PostgresTypeUtilsTest {

    // --------------------------------------------------------------------------------------------
    // Tests for handleNumericWithDecimalMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleNumericPreciseWithinRange() {
        // precision=20 > DEFAULT_SCALE and <= MAX_PRECISION, should produce DECIMAL(20, 5)
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        20, 5, JdbcValueConverters.DecimalMode.PRECISE);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(20, 5));
    }

    @Test
    void testHandleNumericPreciseWithZeroPrecision() {
        // precision=0 (no explicit precision), should fall back to (MAX_PRECISION, DEFAULT_SCALE)
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        0, 0, JdbcValueConverters.DecimalMode.PRECISE);
        assertThat(result)
                .isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE));
    }

    @Test
    void testHandleNumericPreciseBoundaryAtDefaultScale() {
        // precision == DEFAULT_SCALE is NOT > DEFAULT_SCALE, so falls back to max
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        DecimalType.DEFAULT_SCALE, 2, JdbcValueConverters.DecimalMode.PRECISE);
        assertThat(result)
                .isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE));
    }

    @Test
    void testHandleNumericPreciseJustAboveDefaultScale() {
        // precision = DEFAULT_SCALE + 1 is the smallest value where the condition
        // (precision > DEFAULT_SCALE) becomes true, should use exact precision and scale
        int precision = DecimalType.DEFAULT_SCALE + 1;
        int scale = 0;
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        precision, scale, JdbcValueConverters.DecimalMode.PRECISE);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(precision, scale));
    }

    @Test
    void testHandleNumericPreciseExceedsMaxPrecision() {
        // precision > MAX_PRECISION, should still fall back to max
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        DecimalType.MAX_PRECISION + 1, 2, JdbcValueConverters.DecimalMode.PRECISE);
        assertThat(result)
                .isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, DecimalType.DEFAULT_SCALE));
    }

    @Test
    void testHandleNumericDoubleMode() {
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        10, 5, JdbcValueConverters.DecimalMode.DOUBLE);
        assertThat(result).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    void testHandleNumericStringMode() {
        DataType result =
                PostgresTypeUtils.handleNumericWithDecimalMode(
                        10, 5, JdbcValueConverters.DecimalMode.STRING);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleBinaryWithBinaryMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleBinaryBytesMode() {
        DataType result =
                PostgresTypeUtils.handleBinaryWithBinaryMode(
                        CommonConnectorConfig.BinaryHandlingMode.BYTES);
        assertThat(result).isEqualTo(DataTypes.BYTES());
    }

    @Test
    void testHandleBinaryBase64Mode() {
        DataType result =
                PostgresTypeUtils.handleBinaryWithBinaryMode(
                        CommonConnectorConfig.BinaryHandlingMode.BASE64);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    @Test
    void testHandleBinaryHexMode() {
        DataType result =
                PostgresTypeUtils.handleBinaryWithBinaryMode(
                        CommonConnectorConfig.BinaryHandlingMode.HEX);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleMoneyWithDecimalMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleMoneyPreciseMode() {
        DataType result =
                PostgresTypeUtils.handleMoneyWithDecimalMode(
                        2, JdbcValueConverters.DecimalMode.PRECISE);
        assertThat(result).isEqualTo(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 2));
    }

    @Test
    void testHandleMoneyDoubleMode() {
        DataType result =
                PostgresTypeUtils.handleMoneyWithDecimalMode(
                        2, JdbcValueConverters.DecimalMode.DOUBLE);
        assertThat(result).isEqualTo(DataTypes.DOUBLE());
    }

    @Test
    void testHandleMoneyStringMode() {
        DataType result =
                PostgresTypeUtils.handleMoneyWithDecimalMode(
                        2, JdbcValueConverters.DecimalMode.STRING);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleIntervalWithIntervalHandlingMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleIntervalNumericMode() {
        DataType result =
                PostgresTypeUtils.handleIntervalWithIntervalHandlingMode(
                        PostgresConnectorConfig.IntervalHandlingMode.NUMERIC);
        assertThat(result).isEqualTo(DataTypes.BIGINT());
    }

    @Test
    void testHandleIntervalStringMode() {
        DataType result =
                PostgresTypeUtils.handleIntervalWithIntervalHandlingMode(
                        PostgresConnectorConfig.IntervalHandlingMode.STRING);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleDateWithTemporalMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleDateAdaptiveMode() {
        DataType result =
                PostgresTypeUtils.handleDateWithTemporalMode(TemporalPrecisionMode.ADAPTIVE);
        assertThat(result).isEqualTo(DataTypes.DATE());
    }

    @Test
    void testHandleDateAdaptiveTimeMicrosecondsMode() {
        DataType result =
                PostgresTypeUtils.handleDateWithTemporalMode(
                        TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        assertThat(result).isEqualTo(DataTypes.DATE());
    }

    @Test
    void testHandleDateConnectMode() {
        DataType result =
                PostgresTypeUtils.handleDateWithTemporalMode(TemporalPrecisionMode.CONNECT);
        assertThat(result).isEqualTo(DataTypes.DATE());
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleTimeWithTemporalMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleTimeAdaptiveMode() {
        DataType result =
                PostgresTypeUtils.handleTimeWithTemporalMode(TemporalPrecisionMode.ADAPTIVE, 6);
        assertThat(result).isEqualTo(DataTypes.TIME(6));
    }

    @Test
    void testHandleTimeAdaptiveTimeMicrosecondsMode() {
        DataType result =
                PostgresTypeUtils.handleTimeWithTemporalMode(
                        TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS, 3);
        assertThat(result).isEqualTo(DataTypes.TIME(3));
    }

    @Test
    void testHandleTimeConnectMode() {
        DataType result =
                PostgresTypeUtils.handleTimeWithTemporalMode(TemporalPrecisionMode.CONNECT, 0);
        assertThat(result).isEqualTo(DataTypes.TIME(0));
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleTimestampWithTemporalMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleTimestampAdaptiveMode() {
        DataType result =
                PostgresTypeUtils.handleTimestampWithTemporalMode(
                        TemporalPrecisionMode.ADAPTIVE, 6);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(6));
    }

    @Test
    void testHandleTimestampAdaptiveTimeMicrosecondsMode() {
        DataType result =
                PostgresTypeUtils.handleTimestampWithTemporalMode(
                        TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS, 3);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(3));
    }

    @Test
    void testHandleTimestampConnectMode() {
        DataType result =
                PostgresTypeUtils.handleTimestampWithTemporalMode(TemporalPrecisionMode.CONNECT, 0);
        assertThat(result).isEqualTo(DataTypes.TIMESTAMP(0));
    }

    @Test
    void testHandleTimestampScalePreserved() {
        // Verify that scale (fractional seconds precision) is correctly passed through
        for (int scale = 0; scale <= 6; scale++) {
            DataType result =
                    PostgresTypeUtils.handleTimestampWithTemporalMode(
                            TemporalPrecisionMode.ADAPTIVE, scale);
            assertThat(result).isEqualTo(DataTypes.TIMESTAMP(scale));
        }
    }

    @Test
    void testHandleTimeScalePreserved() {
        // Verify that scale (fractional seconds precision) is correctly passed through
        for (int scale = 0; scale <= 6; scale++) {
            DataType result =
                    PostgresTypeUtils.handleTimeWithTemporalMode(
                            TemporalPrecisionMode.ADAPTIVE, scale);
            assertThat(result).isEqualTo(DataTypes.TIME(scale));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Tests for handleHstoreWithHstoreMode
    // --------------------------------------------------------------------------------------------

    @Test
    void testHandleHstoreJsonMode() {
        DataType result =
                PostgresTypeUtils.handleHstoreWithHstoreMode(
                        PostgresConnectorConfig.HStoreHandlingMode.JSON);
        assertThat(result).isEqualTo(DataTypes.STRING());
    }

    @Test
    void testHandleHstoreMapMode() {
        DataType result =
                PostgresTypeUtils.handleHstoreWithHstoreMode(
                        PostgresConnectorConfig.HStoreHandlingMode.MAP);
        assertThat(result).isEqualTo(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));
    }
}
