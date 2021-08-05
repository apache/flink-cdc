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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import static com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.toDebeziumConfig;

/** Utilities related to Debezium. */
public class DebeziumUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumUtils.class);

    /**
     * Creates {@link RelationalTableFilters} from configuration. The {@link RelationalTableFilters}
     * can be used to filter tables according to "table.whitelist" and "database.whitelist" options.
     */
    public static RelationalTableFilters createTableFilters(Configuration configuration) {
        io.debezium.config.Configuration debeziumConfig = toDebeziumConfig(configuration);
        final MySqlConnectorConfig mySqlConnectorConfig = new MySqlConnectorConfig(debeziumConfig);
        return mySqlConnectorConfig.getTableFilters();
    }

    /** Creates and opens a new {@link MySqlConnection}. */
    public static MySqlConnection openMySqlConnection(Configuration configuration) {
        MySqlConnection jdbc =
                new MySqlConnection(
                        new MySqlConnection.MySqlConnectionConfiguration(
                                toDebeziumConfig(configuration)));
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
                            return new BinlogOffset(binlogFilename, binlogPosition);
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
