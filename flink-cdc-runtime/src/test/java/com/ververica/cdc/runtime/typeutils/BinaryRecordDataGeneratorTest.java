/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.typeutils;

import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.LocalZonedTimestampData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.ZonedTimestampData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BinaryRecordDataGenerator}. */
public class BinaryRecordDataGeneratorTest {

    @Test
    void testOf() {
        RowType rowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.BINARY(3),
                        DataTypes.VARBINARY(10),
                        DataTypes.BYTES(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(6, 3),
                        DataTypes.CHAR(5),
                        DataTypes.VARCHAR(10),
                        DataTypes.STRING(),
                        DataTypes.DATE(),
                        DataTypes.TIME(),
                        DataTypes.TIME(6),
                        DataTypes.TIMESTAMP(),
                        DataTypes.TIMESTAMP(3),
                        DataTypes.TIMESTAMP_LTZ(),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_TZ(),
                        DataTypes.TIMESTAMP_TZ(3),
                        DataTypes.ROW(
                                DataTypes.FIELD("t1", DataTypes.STRING()),
                                DataTypes.FIELD("t2", DataTypes.BIGINT())),
                        DataTypes.STRING());

        Object[] testData =
                new Object[] {
                    true,
                    new byte[] {1, 2},
                    new byte[] {3, 4},
                    new byte[] {5, 6, 7},
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    5.1f,
                    6.2,
                    DecimalData.fromBigDecimal(new BigDecimal(7.123), 6, 3),
                    BinaryStringData.fromString("test1"),
                    BinaryStringData.fromString("test2"),
                    BinaryStringData.fromString("test3"),
                    100,
                    200,
                    300,
                    TimestampData.fromMillis(100, 1),
                    TimestampData.fromMillis(200, 0),
                    LocalZonedTimestampData.fromEpochMillis(300, 1),
                    LocalZonedTimestampData.fromEpochMillis(400),
                    ZonedTimestampData.of(500, 1, "UTC"),
                    ZonedTimestampData.of(600, 0, "UTC"),
                    new BinaryRecordDataGenerator(
                                    RowType.of(DataTypes.STRING(), DataTypes.BIGINT()))
                            .generate(new Object[] {BinaryStringData.fromString("test"), 23L}),
                    null
                };
        BinaryRecordData actual = new BinaryRecordDataGenerator(rowType).generate(testData);

        assertThat(actual.getBoolean(0)).isTrue();

        assertThat(actual.getBinary(1)).containsExactly((byte[]) testData[1]);
        assertThat(actual.getBinary(2)).containsExactly((byte[]) testData[2]);
        assertThat(actual.getBinary(3)).containsExactly((byte[]) testData[3]);

        assertThat(actual.getByte(4)).isEqualTo(testData[4]);
        assertThat(actual.getShort(5)).isEqualTo(testData[5]);
        assertThat(actual.getInt(6)).isEqualTo(testData[6]);
        assertThat(actual.getLong(7)).isEqualTo(testData[7]);
        assertThat(actual.getFloat(8)).isEqualTo(testData[8]);
        assertThat(actual.getDouble(9)).isEqualTo(testData[9]);
        assertThat(actual.getDecimal(10, 6, 3)).isEqualTo(testData[10]);

        assertThat(actual.getString(11)).isEqualTo(BinaryStringData.fromString("test1"));
        assertThat(actual.getString(12)).isEqualTo(BinaryStringData.fromString("test2"));
        assertThat(actual.getString(13)).isEqualTo(BinaryStringData.fromString("test3"));

        assertThat(actual.getInt(14)).isEqualTo(testData[14]);
        assertThat(actual.getInt(15)).isEqualTo(testData[15]);
        assertThat(actual.getInt(16)).isEqualTo(testData[16]);

        assertThat(actual.getTimestamp(17, TimestampType.DEFAULT_PRECISION))
                .isEqualTo(testData[17]);
        assertThat(actual.getTimestamp(18, 3)).isEqualTo(testData[18]);
        assertThat(actual.getLocalZonedTimestampData(19, LocalZonedTimestampType.DEFAULT_PRECISION))
                .isEqualTo(testData[19]);
        assertThat(actual.getLocalZonedTimestampData(20, 3)).isEqualTo(testData[20]);
        assertThat(actual.getZonedTimestamp(21, ZonedTimestampType.DEFAULT_PRECISION))
                .isEqualTo(testData[21]);
        assertThat(actual.getZonedTimestamp(22, 3)).isEqualTo(testData[22]);

        assertThat(actual.getRow(23, 2).getString(0))
                .isEqualTo(BinaryStringData.fromString("test"));
        assertThat(actual.getRow(23, 2).getLong(1)).isEqualTo(23L);
        assertThat(actual.isNullAt(24)).isTrue();
    }
}
