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

package org.apache.flink.cdc.common.types.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link org.apache.flink.cdc.common.types.utils.DataTypeUtils}. */
class DataTypeUtilsTest {
    private static final DataType[] ALL_TYPES =
            new DataType[] {
                DataTypes.BOOLEAN(),
                DataTypes.BYTES(),
                DataTypes.BINARY(10),
                DataTypes.VARBINARY(10),
                DataTypes.CHAR(10),
                DataTypes.VARCHAR(10),
                DataTypes.STRING(),
                DataTypes.INT(),
                DataTypes.TINYINT(),
                DataTypes.SMALLINT(),
                DataTypes.BIGINT(),
                DataTypes.DOUBLE(),
                DataTypes.FLOAT(),
                DataTypes.DECIMAL(6, 3),
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIME(6),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP(6),
                DataTypes.TIMESTAMP_LTZ(),
                DataTypes.TIMESTAMP_LTZ(6),
                DataTypes.TIMESTAMP_TZ(),
                DataTypes.TIMESTAMP_TZ(6),
                DataTypes.ARRAY(DataTypes.BIGINT()),
                DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.STRING()),
                DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.STRING()),
                        DataTypes.FIELD("f2", DataTypes.STRING(), "desc")),
                DataTypes.ROW(DataTypes.SMALLINT(), DataTypes.STRING())
            };

    private static final org.apache.flink.table.types.DataType[] FLINK_TYPES =
            new org.apache.flink.table.types.DataType[] {
                BOOLEAN(),
                BYTES(),
                BINARY(10),
                VARBINARY(10),
                CHAR(10),
                VARCHAR(10),
                STRING(),
                INT(),
                TINYINT(),
                SMALLINT(),
                BIGINT(),
                DOUBLE(),
                FLOAT(),
                DECIMAL(6, 3),
                DATE(),
                TIME(),
                TIME(6),
                TIMESTAMP(),
                TIMESTAMP(6),
                TIMESTAMP_LTZ(),
                TIMESTAMP_LTZ(6),
                TIMESTAMP_WITH_TIME_ZONE(),
                TIMESTAMP_WITH_TIME_ZONE(6),
                ARRAY(BIGINT()),
                MAP(SMALLINT(), STRING()),
                ROW(FIELD("f1", STRING()), FIELD("f2", STRING(), "desc")),
                ROW(SMALLINT(), STRING())
            };

    @Test
    void testToFlinkDataType() {
        DataType cdcNullableDataType =
                new RowType(
                        IntStream.range(0, ALL_TYPES.length)
                                .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i]))
                                .collect(Collectors.toList()));
        DataType cdcNotNullDataType =
                new RowType(
                        IntStream.range(0, ALL_TYPES.length)
                                .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i].notNull()))
                                .collect(Collectors.toList()));

        org.apache.flink.table.types.DataType nullableDataType =
                DataTypeUtils.toFlinkDataType(cdcNullableDataType);
        org.apache.flink.table.types.DataType notNullDataType =
                DataTypeUtils.toFlinkDataType(cdcNotNullDataType);

        org.apache.flink.table.types.DataType expectedNullableDataType =
                ROW(
                        IntStream.range(0, FLINK_TYPES.length)
                                .mapToObj(i -> FIELD("f" + i, FLINK_TYPES[i]))
                                .collect(Collectors.toList()));
        org.apache.flink.table.types.DataType expectedNotNullDataType =
                ROW(
                        IntStream.range(0, FLINK_TYPES.length)
                                .mapToObj(i -> FIELD("f" + i, FLINK_TYPES[i].notNull()))
                                .collect(Collectors.toList()));

        assertThat(nullableDataType).isEqualTo(expectedNullableDataType);
        assertThat(notNullDataType).isEqualTo(expectedNotNullDataType);
    }
}
