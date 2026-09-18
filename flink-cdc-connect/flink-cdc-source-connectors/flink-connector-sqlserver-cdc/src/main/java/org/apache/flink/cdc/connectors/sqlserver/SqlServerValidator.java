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

package org.apache.flink.cdc.connectors.sqlserver;

import org.apache.flink.cdc.debezium.Validator;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Validator for SqlServer to validate: SqlServer CDC mechanism is enabled or not, SqlServer version
 * is supported or not.
 */
public class SqlServerValidator implements Validator {

    private static final long serialVersionUID = 1L;
    private final Properties properties;

    public SqlServerValidator(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void validate() {
        try (Connection connection = openConnection(properties);
                PreparedStatement preparedStatement =
                        connection.prepareStatement(
                                "select 1 from sys.databases where name= ? AND is_cdc_enabled=1")) {
            checkVersion(connection);
            checkCdcEnabled(preparedStatement);
        } catch (SQLException ex) {
            throw new TableException(
                    "Unexpected error while connecting to SqlServer and validating", ex);
        }
    }

    private void checkCdcEnabled(PreparedStatement preparedStatement) throws SQLException {
        String dbname = properties.getProperty("database.dbname");
        preparedStatement.setString(1, dbname);
        if (!preparedStatement.executeQuery().next()) {
            throw new ValidationException(
                    String.format("SqlServer database %s do not enable cdc.", dbname));
        }
    }

    private void checkVersion(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        // For more information on sqlserver version, please refer to
        // https://docs.microsoft.com/en-us/troubleshoot/sql/general/determine-version-edition-update-level.
        if (metaData.getDatabaseMajorVersion() < 11) {
            throw new ValidationException(
                    String.format(
                            "Currently Flink SqlServer CDC connector only supports SqlServer "
                                    + "whose version is larger or equal to 11, but actual is %d.",
                            metaData.getDatabaseMajorVersion()));
        }
    }

    public static Connection openConnection(Properties properties) throws SQLException {
        DriverManager.registerDriver(new SQLServerDriver());
        String hostname = properties.getProperty("database.hostname");
        String port = properties.getProperty("database.port");
        String dbname = properties.getProperty("database.dbname");
        String userName = properties.getProperty("database.user");
        String userpwd = properties.getProperty("database.password");
        StringBuilder url =
                new StringBuilder("jdbc:sqlserver://")
                        .append(hostname)
                        .append(":")
                        .append(port)
                        .append(";databaseName=")
                        .append(dbname);
        // This validator opens its own connection rather than reusing Debezium's, so it also
        // has to carry the connection settings Debezium is given. mssql-jdbc 10.2 -- the
        // version Debezium 2.0 pulls in, replacing 9.4 -- flipped the "encrypt" default from
        // false to true, so without these the validator fails the TLS handshake against a
        // server with a self-signed certificate even though the connector itself connects
        // fine, and every non-incremental SqlServer source dies in open() before the engine
        // ever starts.
        appendIfPresent(url, properties, "database.encrypt", "encrypt");
        appendIfPresent(
                url, properties, "database.trustServerCertificate", "trustServerCertificate");
        return DriverManager.getConnection(url.toString(), userName, userpwd);
    }

    private static void appendIfPresent(
            StringBuilder url, Properties properties, String propertyName, String urlName) {
        String value = properties.getProperty(propertyName);
        if (value != null && !value.isEmpty()) {
            url.append(";").append(urlName).append("=").append(value);
        }
    }
}
