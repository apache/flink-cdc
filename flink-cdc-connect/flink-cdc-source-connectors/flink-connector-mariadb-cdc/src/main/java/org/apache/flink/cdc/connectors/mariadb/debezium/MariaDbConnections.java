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

package org.apache.flink.cdc.connectors.mariadb.debezium;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnection;

import java.sql.SQLException;

/**
 * MariaDB-specific JDBC helpers.
 *
 * <p>Keeping these in the MariaDB module avoids shadowing the whole {@code MySqlConnection} class
 * just to add a single MariaDB query: MariaDB exposes the executed GTID set through
 * {@code @@gtid_binlog_pos} rather than MySQL's {@code @@global.gtid_executed}.
 */
public final class MariaDbConnections {

    private MariaDbConnections() {}

    /**
     * Reads the GTID set present in the MariaDB server's binary log via {@code @@gtid_binlog_pos}.
     * Returns an empty string when the server reports none.
     */
    public static String gtidExecuted(MySqlConnection jdbc) {
        try {
            String value =
                    jdbc.queryAndMap(
                            "SELECT @@gtid_binlog_pos", rs -> rs.next() ? rs.getString(1) : null);
            return value == null ? "" : value;
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Unexpected error while reading MariaDB @@gtid_binlog_pos", e);
        }
    }
}
