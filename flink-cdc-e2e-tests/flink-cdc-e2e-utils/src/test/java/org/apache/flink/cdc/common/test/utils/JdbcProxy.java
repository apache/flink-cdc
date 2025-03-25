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

package org.apache.flink.cdc.common.test.utils;

import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Proxy to communicate with database using JDBC protocol. */
public class JdbcProxy {

    private final String url;
    private final String userName;
    private final String password;
    private final String driverClass;

    public JdbcProxy(String url, String userName, String password, String driverClass) {
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.driverClass = driverClass;
    }

    private void checkResult(List<String> expectedResult, String table, String[] fields)
            throws SQLException, ClassNotFoundException {
        Class.forName(driverClass);
        try (Connection dbConn = DriverManager.getConnection(url, userName, password);
                PreparedStatement statement = dbConn.prepareStatement("SELECT * FROM " + table);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                List<String> result = new ArrayList<>();
                for (String field : fields) {
                    Object value = resultSet.getObject(field);
                    if (value == null) {
                        result.add("null");
                    } else {
                        result.add(value.toString());
                    }
                }

                results.add(StringUtils.join(result, ","));
            }
            Collections.sort(results);
            Collections.sort(expectedResult);
            // make it easier to check the result
            Assertions.assertThat(expectedResult.toArray()).isEqualTo(results.toArray());
        }
    }

    /**
     * Check the result of a table with specified fields. If the result is not as expected, it will
     * retry until timeout.
     */
    public void checkResultWithTimeout(
            List<String> expectedResult, String table, String[] fields, long timeout)
            throws Exception {
        long endTimeout = System.currentTimeMillis() + timeout;
        boolean result = false;
        while (System.currentTimeMillis() < endTimeout) {
            try {
                checkResult(expectedResult, table, fields);
                result = true;
                break;
            } catch (AssertionError | SQLException throwable) {
                Thread.sleep(1000L);
            }
        }
        if (!result) {
            checkResult(expectedResult, table, fields);
        }
    }
}
