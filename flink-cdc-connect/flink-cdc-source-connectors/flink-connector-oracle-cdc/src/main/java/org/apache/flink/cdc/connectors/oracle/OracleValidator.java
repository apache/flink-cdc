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

package org.apache.flink.cdc.connectors.oracle;

import org.apache.flink.cdc.connectors.oracle.util.OracleJdbcUrlUtils;
import org.apache.flink.cdc.debezium.Validator;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** Validates the version of the database connecting to. */
public class OracleValidator implements Validator {

    private static final long serialVersionUID = 1L;

    private final Properties properties;

    private static final List<Integer> SUPPORT_VERSIONS = Arrays.asList(11, 12, 19, 21);

    public OracleValidator(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void validate() {
        try (Connection connection = openConnection(properties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (!SUPPORT_VERSIONS.contains(metaData.getDatabaseMajorVersion())) {
                throw new ValidationException(
                        String.format(
                                "Currently Flink Oracle CDC connector only supports Oracle "
                                        + "whose version is either %s but actual is %d.%d.",
                                SUPPORT_VERSIONS,
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
