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

package org.apache.flink.cdc.connectors.mysql.sink.type;

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
import org.apache.flink.cdc.connectors.jdbc.catalog.JdbcColumn;

import com.mysql.cj.MysqlType;

/** MySQL type transformer from {@DataType}. */
public class MySqlTypeTransformer extends DataTypeDefaultVisitor<JdbcColumn.Builder> {
    private final JdbcColumn.Builder builder;

    public MySqlTypeTransformer(JdbcColumn.Builder builder) {
        this.builder = builder;
    }

    @Override
    public JdbcColumn.Builder visit(CharType charType) {
        builder.length(charType.getLength());
        builder.dataType(MysqlType.CHAR.name());
        builder.columnType(charType.asSerializableString());
        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(VarCharType varCharType) {
        builder.length(varCharType.getLength());
        builder.dataType(MysqlType.VARCHAR.name());
        builder.columnType(varCharType.asSerializableString());
        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(BooleanType booleanType) {
        builder.dataType(MysqlType.TINYINT.name());
        builder.columnType("TINYINT(1)");
        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(DecimalType decimalType) {
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();

        builder.dataType(MysqlType.DECIMAL.name());
        builder.columnType(decimalType.asSerializableString());
        builder.length(precision);
        builder.scale(scale);

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(TinyIntType tinyIntType) {
        builder.dataType(MysqlType.TINYINT.name());
        builder.columnType(tinyIntType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(SmallIntType smallIntType) {
        builder.dataType(MysqlType.SMALLINT.name());
        builder.columnType(smallIntType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(IntType intType) {
        builder.dataType(MysqlType.INT.name());
        builder.columnType(intType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(BigIntType bigIntType) {
        builder.dataType(MysqlType.BIGINT.name());
        builder.columnType(bigIntType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(FloatType floatType) {
        builder.dataType(MysqlType.FLOAT.name());
        builder.columnType(floatType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(DoubleType doubleType) {
        builder.dataType(MysqlType.DOUBLE.name());
        builder.columnType(doubleType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(BinaryType binaryType) {
        builder.dataType(MysqlType.BINARY.name());
        builder.length(binaryType.getLength());
        builder.columnType(binaryType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(VarBinaryType bytesType) {
        builder.dataType(MysqlType.BINARY.name());
        builder.length(bytesType.getLength());
        builder.columnType(bytesType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(DateType dateType) {
        builder.dataType(MysqlType.DATE.name());
        builder.columnType(dateType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(TimeType timeType) {
        builder.length(timeType.getPrecision());
        builder.dataType(MysqlType.TIME.name());
        builder.columnType(timeType.asSerializableString());

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(TimestampType timestampType) {
        int precision = timestampType.getPrecision();
        builder.dataType(MysqlType.DATETIME.name());
        builder.columnType(MysqlType.TIMESTAMP.name());
        builder.columnType(timestampType.asSerializableString());
        builder.length(precision);

        return builder;
    }

    @Override
    public JdbcColumn.Builder visit(LocalZonedTimestampType localZonedTimestampType) {
        int precision = localZonedTimestampType.getPrecision();
        builder.dataType(MysqlType.TIMESTAMP.name());
        builder.length(precision);
        builder.columnType(localZonedTimestampType.asSerializableString());

        return builder;
    }

    @Override
    protected JdbcColumn.Builder defaultMethod(DataType dataType) {
        return null;
    }
}
