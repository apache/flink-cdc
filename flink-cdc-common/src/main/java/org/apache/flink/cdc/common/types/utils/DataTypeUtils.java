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

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.MapData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.util.CollectionUtil;

import java.util.List;
import java.util.stream.Collectors;

/** Utilities for handling {@link DataType}s. */
public class DataTypeUtils {
    /**
     * Returns the conversion class for the given {@link DataType} that is used by the table runtime
     * as internal data structure.
     */
    public static Class<?> toInternalConversionClass(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return DecimalData.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.class;
            case TIMESTAMP_WITH_TIME_ZONE:
                return ZonedTimestampData.class;
            case ARRAY:
                return ArrayData.class;
            case MAP:
                return MapData.class;
            case ROW:
                return RecordData.class;
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Convert CDC's {@link DataType} to Flink's internal {@link
     * org.apache.flink.table.types.DataType}.
     */
    public static org.apache.flink.table.types.DataType toFlinkDataType(DataType type) {
        // ordered by type root definition
        List<DataType> children = type.getChildren();
        int length = DataTypes.getLength(type).orElse(0);
        int precision = DataTypes.getPrecision(type).orElse(0);
        int scale = DataTypes.getScale(type).orElse(0);
        switch (type.getTypeRoot()) {
            case CHAR:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.CHAR(length)
                        : org.apache.flink.table.api.DataTypes.CHAR(length).notNull();
            case VARCHAR:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.VARCHAR(length)
                        : org.apache.flink.table.api.DataTypes.VARCHAR(length).notNull();
            case BOOLEAN:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.BOOLEAN()
                        : org.apache.flink.table.api.DataTypes.BOOLEAN().notNull();
            case BINARY:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.BINARY(length)
                        : org.apache.flink.table.api.DataTypes.BINARY(length).notNull();
            case VARBINARY:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.VARBINARY(length)
                        : org.apache.flink.table.api.DataTypes.VARBINARY(length).notNull();
            case DECIMAL:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.DECIMAL(precision, scale)
                        : org.apache.flink.table.api.DataTypes.DECIMAL(precision, scale).notNull();
            case TINYINT:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.TINYINT()
                        : org.apache.flink.table.api.DataTypes.TINYINT().notNull();
            case SMALLINT:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.SMALLINT()
                        : org.apache.flink.table.api.DataTypes.SMALLINT().notNull();
            case INTEGER:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.INT()
                        : org.apache.flink.table.api.DataTypes.INT().notNull();
            case DATE:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.DATE()
                        : org.apache.flink.table.api.DataTypes.DATE().notNull();
            case TIME_WITHOUT_TIME_ZONE:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.TIME(precision)
                        : org.apache.flink.table.api.DataTypes.TIME(precision).notNull();
            case BIGINT:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.BIGINT()
                        : org.apache.flink.table.api.DataTypes.BIGINT().notNull();
            case FLOAT:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.FLOAT()
                        : org.apache.flink.table.api.DataTypes.FLOAT().notNull();
            case DOUBLE:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.DOUBLE()
                        : org.apache.flink.table.api.DataTypes.DOUBLE().notNull();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.TIMESTAMP(precision)
                        : org.apache.flink.table.api.DataTypes.TIMESTAMP(precision).notNull();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                precision)
                        : org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                        precision)
                                .notNull();
            case TIMESTAMP_WITH_TIME_ZONE:
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE(precision)
                        : org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE(precision)
                                .notNull();
            case ARRAY:
                Preconditions.checkState(children != null && !children.isEmpty());
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.ARRAY(
                                toFlinkDataType(children.get(0)))
                        : org.apache.flink.table.api.DataTypes.ARRAY(
                                        toFlinkDataType(children.get(0)))
                                .notNull();
            case MAP:
                Preconditions.checkState(children != null && children.size() > 1);
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.MAP(
                                toFlinkDataType(children.get(0)), toFlinkDataType(children.get(1)))
                        : org.apache.flink.table.api.DataTypes.MAP(
                                        toFlinkDataType(children.get(0)),
                                        toFlinkDataType(children.get(1)))
                                .notNull();
            case ROW:
                Preconditions.checkState(!CollectionUtil.isNullOrEmpty(children));
                RowType rowType = (RowType) type;
                List<org.apache.flink.table.api.DataTypes.Field> fields =
                        rowType.getFields().stream()
                                .map(DataField::toFlinkDataTypeField)
                                .collect(Collectors.toList());
                return type.isNullable()
                        ? org.apache.flink.table.api.DataTypes.ROW(fields)
                        : org.apache.flink.table.api.DataTypes.ROW(fields).notNull();
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }
}
