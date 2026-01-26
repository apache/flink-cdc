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

package org.apache.flink.cdc.runtime.typeutils;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.MapType;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.VariantType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;

/** A data type converter. */
public class CalciteDataTypeConverter {

    public static RelDataType convertCalciteRelDataType(
            RelDataTypeFactory typeFactory, List<Column> columns) {
        RelDataTypeFactory.Builder fieldInfoBuilder = typeFactory.builder();
        for (Column column : columns) {
            switch (column.getType().getTypeRoot()) {
                case BOOLEAN:
                    BooleanType booleanType = (BooleanType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.BOOLEAN)
                            .nullable(booleanType.isNullable());
                    break;
                case TINYINT:
                    TinyIntType tinyIntType = (TinyIntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.TINYINT)
                            .nullable(tinyIntType.isNullable());
                    break;
                case SMALLINT:
                    SmallIntType smallIntType = (SmallIntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.SMALLINT)
                            .nullable(smallIntType.isNullable());
                    break;
                case INTEGER:
                    IntType intType = (IntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.INTEGER)
                            .nullable(intType.isNullable());
                    break;
                case BIGINT:
                    BigIntType bigIntType = (BigIntType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.BIGINT)
                            .nullable(bigIntType.isNullable());
                    break;
                case DATE:
                    DateType dataType = (DateType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.DATE)
                            .nullable(dataType.isNullable());
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                    TimeType timeType = (TimeType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.TIME, timeType.getPrecision())
                            .nullable(timeType.isNullable());
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    TimestampType timestampType = (TimestampType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.TIMESTAMP,
                                    timestampType.getPrecision())
                            .nullable(timestampType.isNullable());
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    ZonedTimestampType zonedTimestampType = (ZonedTimestampType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.TIMESTAMP,
                                    zonedTimestampType.getPrecision())
                            .nullable(zonedTimestampType.isNullable());
                    break;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    LocalZonedTimestampType localZonedTimestampType =
                            (LocalZonedTimestampType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                                    localZonedTimestampType.getPrecision())
                            .nullable(localZonedTimestampType.isNullable());
                    break;
                case FLOAT:
                    FloatType floatType = (FloatType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.FLOAT)
                            .nullable(floatType.isNullable());
                    break;
                case DOUBLE:
                    DoubleType doubleType = (DoubleType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.DOUBLE)
                            .nullable(doubleType.isNullable());
                    break;
                case CHAR:
                    CharType charType = (CharType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.CHAR, charType.getLength())
                            .nullable(charType.isNullable());
                    break;
                case VARCHAR:
                    VarCharType varCharType = (VarCharType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.VARCHAR, varCharType.getLength())
                            .nullable(varCharType.isNullable());
                    break;
                case BINARY:
                    BinaryType binaryType = (BinaryType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.BINARY, binaryType.getLength())
                            .nullable(binaryType.isNullable());
                    break;
                case VARBINARY:
                    VarBinaryType varBinaryType = (VarBinaryType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.VARBINARY, varBinaryType.getLength())
                            .nullable(varBinaryType.isNullable());
                    break;
                case DECIMAL:
                    DecimalType decimalType = (DecimalType) column.getType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    SqlTypeName.DECIMAL,
                                    decimalType.getPrecision(),
                                    decimalType.getScale())
                            .nullable(decimalType.isNullable());
                    break;
                case ROW:
                    RowType rowType = (RowType) column.getType();
                    List<RelDataType> dataTypes =
                            rowType.getFieldTypes().stream()
                                    .map((type) -> convertCalciteType(typeFactory, type))
                                    .collect(Collectors.toList());
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    typeFactory.createStructType(
                                            dataTypes, rowType.getFieldNames()))
                            .nullable(rowType.isNullable());
                    break;
                case ARRAY:
                    ArrayType arrayType = (ArrayType) column.getType();
                    DataType elementType = arrayType.getElementType();
                    fieldInfoBuilder
                            .add(
                                    column.getName(),
                                    typeFactory.createArrayType(
                                            convertCalciteType(typeFactory, elementType), -1))
                            .nullable(arrayType.isNullable());
                    break;
                case MAP:
                    MapType mapType = (MapType) column.getType();
                    RelDataType keyType = convertCalciteType(typeFactory, mapType.getKeyType());
                    RelDataType valueType = convertCalciteType(typeFactory, mapType.getValueType());
                    fieldInfoBuilder
                            .add(column.getName(), typeFactory.createMapType(keyType, valueType))
                            .nullable(mapType.isNullable());
                    break;
                case VARIANT:
                    VariantType variantType = (VariantType) column.getType();
                    fieldInfoBuilder
                            .add(column.getName(), SqlTypeName.VARIANT)
                            .nullable(variantType.isNullable());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type: " + column.getType());
            }
        }
        return fieldInfoBuilder.build();
    }

    public static RelDataType convertCalciteType(
            RelDataTypeFactory typeFactory, DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case TINYINT:
                return typeFactory.createSqlType(SqlTypeName.TINYINT);
            case SMALLINT:
                return typeFactory.createSqlType(SqlTypeName.SMALLINT);
            case INTEGER:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case BIGINT:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case DATE:
                return typeFactory.createSqlType(SqlTypeName.DATE);
            case TIME_WITHOUT_TIME_ZONE:
                TimeType timeType = (TimeType) dataType;
                return typeFactory.createSqlType(SqlTypeName.TIME, timeType.getPrecision());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) dataType;
                return typeFactory.createSqlType(
                        SqlTypeName.TIMESTAMP, timestampType.getPrecision());
            case TIMESTAMP_WITH_TIME_ZONE:
                // TODO: Bump Calcite to support its TIMESTAMP_TZ type via #FLINK-37123
                throw new UnsupportedOperationException("Unsupported type: TIMESTAMP_TZ");
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) dataType;
                return typeFactory.createSqlType(
                        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                        localZonedTimestampType.getPrecision());
            case FLOAT:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case CHAR:
                CharType charType = (CharType) dataType;
                return typeFactory.createSqlType(SqlTypeName.CHAR, charType.getLength());
            case VARCHAR:
                VarCharType varCharType = (VarCharType) dataType;
                return typeFactory.createSqlType(SqlTypeName.VARCHAR, varCharType.getLength());
            case BINARY:
                BinaryType binaryType = (BinaryType) dataType;
                return typeFactory.createSqlType(SqlTypeName.BINARY, binaryType.getLength());
            case VARBINARY:
                VarBinaryType varBinaryType = (VarBinaryType) dataType;
                return typeFactory.createSqlType(SqlTypeName.VARBINARY, varBinaryType.getLength());
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return typeFactory.createSqlType(
                        SqlTypeName.DECIMAL, decimalType.getPrecision(), decimalType.getScale());
            case ROW:
                List<RelDataType> dataTypes =
                        ((RowType) dataType)
                                .getFieldTypes().stream()
                                        .map((type) -> convertCalciteType(typeFactory, type))
                                        .collect(Collectors.toList());
                return typeFactory.createStructType(
                        dataTypes, ((RowType) dataType).getFieldNames());
            case ARRAY:
                DataType elementType = ((ArrayType) dataType).getElementType();
                return typeFactory.createArrayType(
                        convertCalciteType(typeFactory, elementType), -1);
            case MAP:
                RelDataType keyType =
                        convertCalciteType(typeFactory, ((MapType) dataType).getKeyType());
                RelDataType valueType =
                        convertCalciteType(typeFactory, ((MapType) dataType).getValueType());
                return typeFactory.createMapType(keyType, valueType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    public static DataType convertCalciteRelDataTypeToDataType(RelDataType relDataType) {
        switch (relDataType.getSqlTypeName()) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INTEGER:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME(relDataType.getPrecision());
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(relDataType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIMESTAMP_LTZ(relDataType.getPrecision());
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case CHAR:
            case VARCHAR:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(relDataType.getPrecision());
            case VARBINARY:
                return DataTypes.VARBINARY(relDataType.getPrecision());
            case DECIMAL:
                return DataTypes.DECIMAL(relDataType.getPrecision(), relDataType.getScale());
            case ARRAY:
                RelDataType componentType = relDataType.getComponentType();
                return DataTypes.ARRAY(convertCalciteRelDataTypeToDataType(componentType));
            case MAP:
                RelDataType keyType = relDataType.getKeyType();
                RelDataType valueType = relDataType.getValueType();
                return DataTypes.MAP(
                        convertCalciteRelDataTypeToDataType(keyType),
                        convertCalciteRelDataTypeToDataType(valueType));
            case ROW:
            case VARIANT:
                return DataTypes.VARIANT();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + relDataType.getSqlTypeName());
        }
    }
}
