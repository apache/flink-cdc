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

package com.ververica.cdc.connectors.mysql.debezium;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/** Utilities related to Debezium. */
public class DebeziumUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumUtils.class);

    /** Creates and opens a new {@link MySqlConnection}. */
    public static MySqlConnection openMySqlConnection(Configuration configuration) {
        MySqlConnection jdbc =
                new MySqlConnection(
                        new MySqlConnection.MySqlConnectionConfiguration(configuration));
        try {
            jdbc.connect();
        } catch (SQLException e) {
            LOG.error("Failed to open MySQL connection", e);
            throw new FlinkRuntimeException("Failed to open MySQL connection", e);
        }

        return jdbc;
    }

    /** Closes the given {@link MySqlConnection}. */
    public static void closeMySqlConnection(MySqlConnection jdbc) {
        try {
            if (jdbc != null) {
                jdbc.close();
            }
        } catch (SQLException e) {
            LOG.error("Failed to close MySQL connection", e);
            throw new FlinkRuntimeException("Failed to close MySQL connection", e);
        }
    }

    /** Fetch current binlog offsets in MySql Server. */
    public static BinlogOffset currentBinlogOffset(MySqlConnection jdbc) {
        final String showMasterStmt = "SHOW MASTER STATUS";
        try {
            return jdbc.queryAndMap(
                    showMasterStmt,
                    rs -> {
                        if (rs.next()) {
                            final String binlogFilename = rs.getString(1);
                            final long binlogPosition = rs.getLong(2);
                            final String gtidSet =
                                    rs.getMetaData().getColumnCount() > 4 ? rs.getString(5) : null;
                            return new BinlogOffset(
                                    binlogFilename, binlogPosition, 0L, 0, 0, gtidSet, null);
                        } else {
                            throw new FlinkRuntimeException(
                                    "Cannot read the binlog filename and position via '"
                                            + showMasterStmt
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot read the binlog filename and position via '"
                            + showMasterStmt
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }
}
