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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSink;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;

/** Util class for {@link IcebergDataSink}. */
public class IcebergTypeUtils {

    /** Convert column from CDC framework to Iceberg framework. */
    public static Types.NestedField convertCDCColumnToIcebergField(
            int index, PhysicalColumn column) {
        DataType dataType = column.getType();
        return Types.NestedField.of(
                index,
                dataType.isNullable(),
                column.getName(),
                convertCDCTypeToIcebergType(dataType),
                column.getComment());
    }

    /** Convert data type from CDC framework to Iceberg framework. */
    public static Type convertCDCTypeToIcebergType(DataType type) {
        // ordered by type root definition
        List<DataType> children = type.getChildren();
        int length = DataTypes.getLength(type).orElse(0);
        int precision = DataTypes.getPrecision(type).orElse(0);
        int scale = DataTypes.getScale(type).orElse(0);
        switch (type.getTypeRoot()) {
            case CHAR:
                return new Types.StringType();
            case VARCHAR:
                return new Types.StringType();
            case BOOLEAN:
                return new Types.BooleanType();
            case BINARY:
                return new Types.BinaryType();
            case VARBINARY:
                return new Types.BinaryType();
            case DECIMAL:
                return Types.DecimalType.of(precision, scale);
            case TINYINT:
                return new Types.IntegerType();
            case SMALLINT:
                return new Types.IntegerType();
            case INTEGER:
                return new Types.IntegerType();
            case DATE:
                return new Types.DateType();
            case TIME_WITHOUT_TIME_ZONE:
                return Types.TimeType.get();
            case BIGINT:
                return new Types.LongType();
            case FLOAT:
                return new Types.FloatType();
            case DOUBLE:
                return new Types.DoubleType();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Types.TimestampType.withoutZone();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Types.TimestampType.withoutZone();
            case TIMESTAMP_WITH_TIME_ZONE:
                return Types.TimestampType.withZone();
            case ARRAY:
                throw new IllegalArgumentException("Illegal type: " + type);
            case MAP:
                throw new IllegalArgumentException("Illegal type: " + type);
            case ROW:
                throw new IllegalArgumentException("Illegal type: " + type);
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }
}
