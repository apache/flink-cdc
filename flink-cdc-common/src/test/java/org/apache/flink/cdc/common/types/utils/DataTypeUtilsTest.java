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

import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.List;
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

    @Test
    void testToFlinkDataType() {
        List<DataField> list =
                IntStream.range(0, ALL_TYPES.length)
                        .mapToObj(i -> DataTypes.FIELD("f" + i, ALL_TYPES[i]))
                        .collect(Collectors.toList());

        org.apache.flink.table.types.DataType dataType =
                DataTypeUtils.toFlinkDataType(new RowType(list));

        org.apache.flink.table.types.DataType expectedDataType =
                ROW(
                        FIELD("f0", BOOLEAN()),
                        FIELD("f1", BYTES()),
                        FIELD("f2", BINARY(10)),
                        FIELD("f3", VARBINARY(10)),
                        FIELD("f4", CHAR(10)),
                        FIELD("f5", VARCHAR(10)),
                        FIELD("f6", STRING()),
                        FIELD("f7", INT()),
                        FIELD("f8", TINYINT()),
                        FIELD("f9", SMALLINT()),
                        FIELD("f10", BIGINT()),
                        FIELD("f11", DOUBLE()),
                        FIELD("f12", FLOAT()),
                        FIELD("f13", DECIMAL(6, 3)),
                        FIELD("f14", DATE()),
                        FIELD("f15", TIME()),
                        FIELD("f16", TIME(6)),
                        FIELD("f17", TIMESTAMP_WITH_TIME_ZONE()),
                        FIELD("f18", TIMESTAMP_WITH_TIME_ZONE(6)),
                        FIELD("f19", TIMESTAMP_LTZ()),
                        FIELD("f20", TIMESTAMP_LTZ(6)),
                        FIELD("f21", TIMESTAMP_WITH_TIME_ZONE()),
                        FIELD("f22", TIMESTAMP_WITH_TIME_ZONE(6)),
                        FIELD("f23", ARRAY(BIGINT())),
                        FIELD("f24", MAP(SMALLINT(), STRING())),
                        FIELD("f25", ROW(FIELD("f1", STRING()), FIELD("f2", STRING(), "desc"))),
                        FIELD("f26", ROW(SMALLINT(), STRING())));

        assertThat(dataType).isEqualTo(expectedDataType);
    }
}
