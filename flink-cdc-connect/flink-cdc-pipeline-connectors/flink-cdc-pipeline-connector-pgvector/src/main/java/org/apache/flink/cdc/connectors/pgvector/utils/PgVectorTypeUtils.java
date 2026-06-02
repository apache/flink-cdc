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

package org.apache.flink.cdc.connectors.pgvector.utils;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.ArrayType;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** Flink CDC to PostgreSQL/pgvector type mapping. */
public class PgVectorTypeUtils {

    private PgVectorTypeUtils() {}

    public static String toPostgresType(
            PgVectorTableInfo tableInfo,
            Column column,
            Map<String, PgVectorColumnSpec> vectorColumns) {
        Optional<PgVectorColumnSpec> vectorColumn =
                findVectorColumnSpec(tableInfo, column.getName(), vectorColumns);
        if (vectorColumn.isPresent()) {
            return vectorColumn.get().toSqlType();
        }
        return toPostgresType(column.getType());
    }

    public static Optional<PgVectorColumnSpec> findVectorColumnSpec(
            PgVectorTableInfo tableInfo,
            String columnName,
            Map<String, PgVectorColumnSpec> vectorColumns) {
        PgVectorColumnSpec spec =
                vectorColumns.get(PgVectorSqlUtils.vectorColumnKey(tableInfo, columnName));
        if (spec != null) {
            return Optional.of(spec);
        }
        return Optional.ofNullable(vectorColumns.get(columnName));
    }

    public static String toPostgresType(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
                return "CHAR(" + ((CharType) dataType).getLength() + ")";
            case VARCHAR:
                int varcharLength = ((VarCharType) dataType).getLength();
                return varcharLength == VarCharType.MAX_LENGTH
                        ? "TEXT"
                        : "VARCHAR(" + varcharLength + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case BINARY:
            case VARBINARY:
                return "BYTEA";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return "DECIMAL("
                        + decimalType.getPrecision()
                        + ", "
                        + decimalType.getScale()
                        + ")";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return "TIME(" + Math.min(((TimeType) dataType).getPrecision(), 6) + ")";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "TIMESTAMP";
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "TIMESTAMPTZ";
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                return toArrayElementType(arrayType.getElementType()) + "[]";
            case MAP:
            case ROW:
            case VARIANT:
                return "TEXT";
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink CDC type for pgvector sink: "
                                + dataType.asSummaryString());
        }
    }

    private static String toArrayElementType(DataType elementType) {
        if (elementType instanceof BinaryType || elementType instanceof VarBinaryType) {
            return "BYTEA";
        }
        String type = toPostgresType(elementType).toUpperCase(Locale.ROOT);
        if (type.endsWith("[]")) {
            return "TEXT";
        }
        return type;
    }

    /** Returns the PostgreSQL type name used by {@link java.sql.Connection#createArrayOf}. */
    public static String toJdbcArrayTypeName(DataType elementType) {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                return "bool";
            case TINYINT:
            case SMALLINT:
                return "int2";
            case INTEGER:
                return "int4";
            case BIGINT:
                return "int8";
            case FLOAT:
                return "float4";
            case DOUBLE:
                return "float8";
            case CHAR:
            case VARCHAR:
            case ARRAY:
            case MAP:
            case ROW:
            case VARIANT:
                return "text";
            case BINARY:
            case VARBINARY:
                return "bytea";
            case DECIMAL:
                return "numeric";
            case DATE:
                return "date";
            case TIME_WITHOUT_TIME_ZONE:
                return "time";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "timestamp";
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "timestamptz";
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink CDC array element type for pgvector sink: "
                                + elementType.asSummaryString());
        }
    }
}
