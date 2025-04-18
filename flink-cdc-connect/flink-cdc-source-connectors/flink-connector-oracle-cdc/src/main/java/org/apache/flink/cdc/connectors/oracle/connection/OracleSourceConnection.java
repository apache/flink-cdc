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

package org.apache.flink.cdc.connectors.oracle.connection;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.function.Supplier;

/** Used to create {@link OracleConnection} specified to Oracle. */
public class OracleSourceConnection extends OracleConnection {

    public OracleSourceConnection(
            JdbcConfiguration config, Supplier<ClassLoader> classLoaderSupplier) {
        super(config, classLoaderSupplier);
    }

    @Override
    public <T extends DatabaseSchema<TableId>> Object getColumnValue(
            ResultSet rs, int columnIndex, Column column, Table table, T schema)
            throws SQLException {
        try {
            if (rs.getObject(columnIndex) == null) {
                return null;
            }
            switch (column.jdbcType()) {
                case Types.FLOAT:
                    return rs.getFloat(columnIndex);
                case Types.DOUBLE:
                    return rs.getDouble(columnIndex);
                default:
                    return rs.getObject(columnIndex);
            }
        } catch (SQLException e) {
            return super.getColumnValue(rs, columnIndex, column, table, schema);
        }
    }
}
