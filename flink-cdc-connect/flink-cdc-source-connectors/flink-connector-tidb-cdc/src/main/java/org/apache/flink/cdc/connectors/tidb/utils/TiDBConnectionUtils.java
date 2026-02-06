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

package org.apache.flink.cdc.connectors.tidb.utils;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/** Utils to obtain the connection of TiDB. */
public class TiDBConnectionUtils {

    public static boolean isTableIdCaseInsensitive(JdbcConnection connection) {
        return !"0"
                .equals(
                        readMySqlSystemVariables(connection)
                                .get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
    }

    public static Map<String, String> readMySqlSystemVariables(JdbcConnection connection) {
        // Read the system variables from the MySQL instance and get the current database name ...
        return querySystemVariables(connection, "SHOW VARIABLES");
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
            throw new FlinkRuntimeException("Error reading TiDB variables: " + e.getMessage(), e);
        }

        return variables;
    }

    // MysqlValueConverters
    public static TiDBValueConverters getValueConverters(TiDBConnectorConfig dbzTiDBConfig) {
        TemporalPrecisionMode timePrecisionMode = dbzTiDBConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = dbzTiDBConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                dbzTiDBConfig.getConfig().getString(dbzTiDBConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        TiDBConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                TiDBConnectorConfig.BigIntUnsignedHandlingMode.parse(bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        boolean timeAdjusterEnabled =
                dbzTiDBConfig.getConfig().getBoolean(dbzTiDBConfig.ENABLE_TIME_ADJUSTER);

        return new TiDBValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                dbzTiDBConfig.binaryHandlingMode(),
                timeAdjusterEnabled ? TiDBValueConverters::adjustTemporal : x -> x,
                TiDBValueConverters::defaultParsingErrorHandler);
    }
}
