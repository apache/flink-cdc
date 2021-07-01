/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import io.debezium.relational.Column;

import java.sql.Types;
import java.util.List;

/** Utils that covert JDBC type to Flink SQL type. */
public class JdbcTypeUtils {

    public static RowType getFlinkType(List<Column> jdbcColumns) {
        LogicalType[] logicalTypes = new LogicalType[jdbcColumns.size()];
        for (int i = 0; i < jdbcColumns.size(); i++) {
            logicalTypes[i] = getFlinkType(jdbcColumns.get(i));
        }
        return RowType.of(logicalTypes);
    }

    private static LogicalType getFlinkType(Column column) {
        switch (column.jdbcType()) {
            case Types.BIGINT:
                return new BigIntType();
            case Types.INTEGER:
                return new IntType();
            case Types.BOOLEAN:
                return new BooleanType();
            case Types.TINYINT:
                return new TinyIntType();
            case Types.SMALLINT:
                return new SmallIntType();
            case Types.VARCHAR:
                return new VarCharType(Integer.MAX_VALUE);
            case Types.REAL:
                return new FloatType();
            case Types.DOUBLE:
                return new DoubleType();
            case Types.DECIMAL:
                return new DecimalType();
            case Types.DATE:
                return new DateType();
            case Types.TIME:
                return new TimeType();
            case Types.TIMESTAMP:
                return new TimestampType();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported JDBC type:" + column.jdbcType());
        }
    }
}
