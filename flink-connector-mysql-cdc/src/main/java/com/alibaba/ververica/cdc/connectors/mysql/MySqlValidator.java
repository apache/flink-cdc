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

package com.alibaba.ververica.cdc.connectors.mysql;

import org.apache.flink.table.api.TableException;

import com.ververica.cdc.debezium.Validator;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static io.debezium.connector.mysql.MySqlConnection.MySqlConnectionConfiguration;

/**
 * The validator for MySQL: it only cares about the version of the database is larger than or equal
 * to 5.7. It also requires the binlog format in the database is ROW.
 *
 * <p>We adapt part of the logic from the Debzium to validate in the compile phase rather than in
 * runtime time.
 */
public class MySqlValidator implements Validator, Serializable {

    private static final long serialVersionUID = 1L;

    private final Properties properties;

    public MySqlValidator(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void validate() {
        Configuration configuration = Configuration.from(properties);

        try (MySqlConnection connection =
                new MySqlConnection(new MySqlConnectionConfiguration(configuration))) {
            if (!isVersionSatisfied(connection)) {
                throw new TableException(
                        "Currently Flink MySql CDC connector only supports MySql whose version is larger or equal to 5.7.");
            }

            MySqlConnectorConfig mySqlConnectorConfig = new MySqlConnectorConfig(configuration);
            if (mySqlConnectorConfig.getSnapshotMode().shouldStream()) {
                if (!isRowFormat(connection)) {
                    throw new TableException(
                            "The MySQL server is not configured to use a ROW binlog_format, which is "
                                    + "required for this connector to work properly. Change the MySQL configuration to use a "
                                    + "binlog_format=ROW and restart the connector.");
                } else if (!isBinlogRowImageFull(connection)) {
                    throw new TableException(
                            "The MySQL server is not configured to use a FULL binlog_row_image, which is "
                                    + "required for this connector to work properly. Change the MySQL configuration to use a "
                                    + "binlog_row_image=FULL and restart the connector.");
                }
            }
        } catch (SQLException ex) {
            throw new TableException("Failed to validate the connection.", ex);
        }
    }

    private boolean isVersionSatisfied(MySqlConnection connection) throws SQLException {
        String version =
                connection.queryAndMap("SELECT VERSION()", rs -> rs.next() ? rs.getString(1) : "");

        // Only care about the major version and minor version
        Integer[] versionNumbers =
                Arrays.stream(version.split("\\."))
                        .limit(2)
                        .map(Integer::new)
                        .toArray(Integer[]::new);
        if (versionNumbers[0] > 5) {
            return true;
        } else if (versionNumbers[0] < 5) {
            return false;
        } else {
            return versionNumbers[1] >= 7;
        }
    }

    /**
     * Adapt code from the {@link MySqlConnection} to validate the row format. Because this method
     * is untouchable from our side and it's better to validate the requirements from the open
     * phase.
     */
    private boolean isRowFormat(MySqlConnection connection) throws SQLException {
        String mode =
                connection.queryAndMap(
                        "SHOW GLOBAL VARIABLES LIKE 'binlog_format'",
                        rs -> rs.next() ? rs.getString(2) : "");
        return "ROW".equals(mode);
    }

    /**
     * Adapt code from the {@link MySqlConnection} to validate the row format. Because this method
     * is untouchable from our side and it's better to validate the requirements from the open
     * phase.
     */
    private boolean isBinlogRowImageFull(MySqlConnection connection) throws SQLException {
        String rowImage =
                connection.queryAndMap(
                        "SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'",
                        rs -> {
                            if (rs.next()) {
                                return rs.getString(2);
                            }
                            // This setting was introduced in MySQL 5.6+ with default of 'FULL'.
                            // For older versions, assume 'FULL'.
                            return "FULL";
                        });
        return "FULL".equalsIgnoreCase(rowImage);
    }
}
