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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.BigIntType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.BooleanType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeDefaultVisitor;
import org.apache.flink.cdc.common.types.DateType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.DoubleType;
import org.apache.flink.cdc.common.types.FloatType;
import org.apache.flink.cdc.common.types.IntType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.SmallIntType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.TinyIntType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalogException;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;
import org.apache.flink.cdc.connectors.oceanbase.utils.ListUtils;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/** Utilities for conversion from source table to OceanBase table. */
public class OceanBaseUtils {

    /** Convert a source table to {@link OceanBaseTable}. */
    public static OceanBaseTable toOceanBaseTable(TableId tableId, Schema schema) {

        List<Column> columns = schema.getColumns();
        List<String> primaryKeys = schema.primaryKeys();
        List<String> partitionKeys = schema.partitionKeys();
        List<OceanBaseColumn> oceanBaseColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            // Since OceanBase partition tables are not yet supported, we first use
            // partitionKeys as part of primaryKeys.
            boolean isPrimaryKeys =
                    primaryKeys.contains(column.getName())
                            || partitionKeys.contains(column.getName());
            // All parts of a PRIMARY KEY must be NOT NULL. if you need NULL in a key, use UNIQUE
            // instead.
            checkPrimaryKeyNotNull(isPrimaryKeys, column.getType().isNullable());

            OceanBaseColumn.Builder builder =
                    new OceanBaseColumn.Builder()
                            .setColumnName(column.getName())
                            .setOrdinalPosition(i)
                            .setDefaultValue(column.getDefaultValueExpression())
                            .setColumnComment(column.getComment());
            toOceanBaseDataType(column, isPrimaryKeys, builder);
            oceanBaseColumns.add(builder.build());
        }

        OceanBaseTable.Builder tableBuilder =
                new OceanBaseTable.Builder()
                        .setDatabaseName(tableId.getSchemaName())
                        .setTableName(tableId.getTableName())
                        .setTableType(
                                CollectionUtils.isNotEmpty(primaryKeys)
                                        ? OceanBaseTable.TableType.PRIMARY_KEY
                                        : OceanBaseTable.TableType.DUPLICATE_KEY)
                        .setColumns(oceanBaseColumns)
                        // Since OceanBase partition tables are not yet supported, we first use
                        // partitionKeys as part of primaryKeys.
                        .setTableKeys(ListUtils.union(primaryKeys, partitionKeys))
                        .setPartitionKeys(schema.partitionKeys())
                        .setComment(schema.comment());
        return tableBuilder.build();
    }

    /** Convert CDC data type to OceanBase data type. */
    public static void toOceanBaseDataType(
            Column cdcColumn, boolean isPrimaryKeys, OceanBaseColumn.Builder builder) {
        CdcDataTypeTransformer dataTypeTransformer =
                new CdcDataTypeTransformer(isPrimaryKeys, builder);
        cdcColumn.getType().accept(dataTypeTransformer);
    }

    // ------------------------------------------------------------------------------------------
    // OceanBase data types
    // ------------------------------------------------------------------------------------------

    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String BINARY = "BINARY";
    public static final String VARBINARY = "VARBINARY";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String TIME = "TIME";
    public static final String JSON = "JSON";
    public static final String TEXT = "TEXT";
    public static final String LONGBLOB = "LONGBLOB";

    /** Max size of char type of OceanBase. */
    public static final int MAX_CHAR_SIZE = 256;

    /** Max size of varchar type of OceanBase. */
    public static final int MAX_VARCHAR_SIZE = 262144;

    /** The max VARBINARY column length is 1048576. */
    public static final int MAX_VARBINARY_SIZE = 1048576;

    /** Transforms CDC {@link DataType} to OceanBase data type. */
    public static class CdcDataTypeTransformer
            extends DataTypeDefaultVisitor<OceanBaseColumn.Builder> {

        private final OceanBaseColumn.Builder builder;
        private final boolean isPrimaryKeys;

        public CdcDataTypeTransformer(boolean isPrimaryKeys, OceanBaseColumn.Builder builder) {
            this.isPrimaryKeys = isPrimaryKeys;
            this.builder = builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(BooleanType booleanType) {
            builder.setDataType(BOOLEAN);
            builder.setNullable(booleanType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(TinyIntType tinyIntType) {
            builder.setDataType(TINYINT);
            builder.setNullable(tinyIntType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(SmallIntType smallIntType) {
            builder.setDataType(SMALLINT);
            builder.setNullable(smallIntType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(IntType intType) {
            builder.setDataType(INT);
            builder.setNullable(intType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(BigIntType bigIntType) {
            builder.setDataType(BIGINT);
            builder.setNullable(bigIntType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(BinaryType binaryType) {
            builder.setDataType(BINARY);
            builder.setNullable(binaryType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(VarBinaryType bytesType) {
            int length = bytesType.getLength();

            // The max VARBINARY column length is 1048576.
            if (length <= MAX_VARBINARY_SIZE) {
                builder.setDataType(VARBINARY);
                builder.setColumnSize(length);
                builder.setNullable(bytesType.isNullable());
            } else {
                builder.setDataType(LONGBLOB);
                builder.setNullable(bytesType.isNullable());
            }

            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(FloatType floatType) {
            builder.setDataType(FLOAT);
            builder.setNullable(floatType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(DoubleType doubleType) {
            builder.setDataType(DOUBLE);
            builder.setNullable(doubleType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(DecimalType decimalType) {
            builder.setDataType(DECIMAL);
            builder.setColumnSize(decimalType.getPrecision());
            builder.setNumericScale(decimalType.getScale());
            builder.setNullable(decimalType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(CharType charType) {
            int length = charType.getLength();
            if (length <= MAX_CHAR_SIZE) {
                builder.setDataType(CHAR);
                builder.setNullable(charType.isNullable());
                builder.setColumnSize(length);
            } else {
                builder.setDataType(VARCHAR);
                builder.setNullable(charType.isNullable());
                builder.setColumnSize(Math.min(length, MAX_VARCHAR_SIZE));
            }
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(VarCharType varCharType) {
            int length = varCharType.getLength();
            builder.setDataType(VARCHAR);
            builder.setNullable(varCharType.isNullable());
            builder.setColumnSize(Math.min(length, MAX_VARCHAR_SIZE));

            // case for string type to avoid row size too large
            if (varCharType.getLength() > MAX_VARCHAR_SIZE) {
                builder.setDataType(TEXT);
                // A text column can't have a default value in OceanBase.
                builder.setDefaultValue(null);
            }
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(DateType dateType) {
            builder.setDataType(DATE);
            builder.setNullable(dateType.isNullable());
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(TimeType timeType) {
            builder.setDataType(TIME);
            builder.setNullable(timeType.isNullable());
            builder.setColumnSize(
                    Math.min(timeType.getPrecision(), TimestampType.DEFAULT_PRECISION));
            return builder;
        }

        /**
         * The maximum precision of the TIMESTAMP and DATETIME type is 6 in OceanBase. 1.
         * TimestampType with timezone mapping to type TIMESTAMP in oceanbase 2. TimestampType
         * without timezone mapping to type DATETIME in oceanbase
         */
        @Override
        public OceanBaseColumn.Builder visit(TimestampType timestampType) {
            builder.setDataType(DATETIME);
            builder.setNullable(timestampType.isNullable());
            builder.setColumnSize(
                    Math.min(timestampType.getPrecision(), TimestampType.DEFAULT_PRECISION));
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(ZonedTimestampType zonedTimestampType) {
            builder.setDataType(TIMESTAMP);
            builder.setNullable(zonedTimestampType.isNullable());
            builder.setColumnSize(
                    Math.min(zonedTimestampType.getPrecision(), TimestampType.DEFAULT_PRECISION));
            return builder;
        }

        @Override
        public OceanBaseColumn.Builder visit(LocalZonedTimestampType localZonedTimestampType) {
            builder.setDataType(TIMESTAMP);
            builder.setNullable(localZonedTimestampType.isNullable());
            builder.setColumnSize(
                    Math.min(
                            localZonedTimestampType.getPrecision(),
                            TimestampType.DEFAULT_PRECISION));
            return builder;
        }

        @Override
        protected OceanBaseColumn.Builder defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException("Unsupported CDC data type " + dataType);
        }
    }

    private static void checkPrimaryKeyNotNull(boolean isPrimaryKeys, boolean dataTypeNullable) {
        if (isPrimaryKeys && dataTypeNullable) {
            throw new OceanBaseCatalogException(
                    "All parts of a PRIMARY KEY must be NOT NULL in OceanBase. "
                            + "if you need NULL in a key, use UNIQUE instead.");
        }
    }
}
