/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import com.ververica.cdc.connectors.oracle.util.OracleJdbcUrlUtils;
import com.ververica.cdc.debezium.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** Validates the version of the database connecting to. */
public class OracleValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(OracleValidator.class);
    private static final long serialVersionUID = 1L;

    private final Properties properties;

    public OracleValidator(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void validate() {
        try (Connection connection = openConnection(properties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (metaData.getDatabaseMajorVersion() != 19
                    && metaData.getDatabaseMajorVersion() != 12
                    && metaData.getDatabaseMajorVersion() != 11) {
                throw new ValidationException(
                        String.format(
                                "Currently Flink Oracle CDC connector only supports Oracle "
                                        + "whose version is either 11, 12 or 19, but actual is %d.%d.",
                                metaData.getDatabaseMajorVersion(),
                                metaData.getDatabaseMinorVersion()));
            }
        } catch (SQLException ex) {
            throw new TableException(
                    "Unexpected error while connecting to Oracle and validating", ex);
        }
    }

    public static Connection openConnection(Properties properties) throws SQLException {
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        String url = OracleJdbcUrlUtils.getConnectionUrlWithSid(properties);
        String userName = properties.getProperty("database.user");
        String userpwd = properties.getProperty("database.password");
        return DriverManager.getConnection(url, userName, userpwd);
    }
}
