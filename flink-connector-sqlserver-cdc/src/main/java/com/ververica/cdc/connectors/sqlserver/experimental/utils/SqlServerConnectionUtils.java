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

package com.ververica.cdc.connectors.sqlserver.experimental.utils;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.sqlserver.experimental.offset.BinlogOffset;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerTopicSelector;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** MySQL connection Utilities. */
public class SqlServerConnectionUtils {

    /** Creates a new {@link SqlServerConnection}, but not open the connection. */
    public static SqlServerConnection createSqlServerConnection(Configuration dbzConfiguration) {
        SqlServerConnectorConfig sqlServerConfig = new SqlServerConnectorConfig(dbzConfiguration);
        return new SqlServerConnection(
                dbzConfiguration, Clock.system(), sqlServerConfig.getSourceTimestampMode(), null);
    }

    /**
     * Creates a new {@link SqlServerDatabaseSchema} to monitor the latest SqlServer database
     * schemas.
     */
    public static SqlServerDatabaseSchema createSqlServerDatabaseSchema(
            SqlServerConnectorConfig dbzSqlServerConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector =
                SqlServerTopicSelector.defaultSelector(dbzSqlServerConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        SqlServerValueConverters valueConverters = getValueConverters(dbzSqlServerConfig);
        return new SqlServerDatabaseSchema(
                dbzSqlServerConfig, valueConverters, topicSelector, schemaNameAdjuster);
    }

    /** Fetch current binlog offsets in SqlServer Server. */
    public static BinlogOffset currentBinlogOffset(JdbcConnection jdbc) {
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

    // --------------------------------------------------------------------------------------------

    private static SqlServerValueConverters getValueConverters(
            SqlServerConnectorConfig dbzSqlServerConfig) {
        return new SqlServerValueConverters(
                dbzSqlServerConfig.getDecimalMode(),
                dbzSqlServerConfig.getTemporalPrecisionMode(),
                dbzSqlServerConfig.binaryHandlingMode());
    }

    public static boolean isTableIdCaseSensitive(JdbcConnection connection) {
        return false;
    }

    private static Map<String, String> querySystemVariables(
            JdbcConnection connection, String statement) {
        final Map<String, String> variables = new HashMap<>();
        try {
            connection.query(
                    statement,
                    rs -> {
                        while (rs.next()) {
                            String varName = rs.getString(1);
                            String value = rs.getString(2);
                            if (varName != null && value != null) {
                                variables.put(varName, value);
                            }
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error reading MySQL variables: " + e.getMessage(), e);
        }

        return variables;
    }
}
