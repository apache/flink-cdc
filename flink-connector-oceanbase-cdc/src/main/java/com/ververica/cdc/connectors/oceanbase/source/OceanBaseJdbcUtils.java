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

package com.ververica.cdc.connectors.oceanbase.source;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** OceanBase Jdbc utils. */
public class OceanBaseJdbcUtils {

    public static List<String> getChunkKeyColumns(
            OceanBaseDataSource dataSource,
            OceanBaseDialect dialect,
            String dbName,
            String tableName)
            throws SQLException {
        List<String> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(dialect.getQueryPrimaryKeySql(dbName, tableName));
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    public static List<String> getTables(
            OceanBaseDataSource dataSource,
            OceanBaseDialect dialect,
            String dbPattern,
            String tbPattern)
            throws SQLException {
        List<String> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (dialect instanceof OceanBaseMysqlDialect) {
                List<String> dbNames = getResultList(metaData.getCatalogs(), "TABLE_CAT");
                dbNames =
                        dbNames.stream()
                                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                                .collect(Collectors.toList());
                for (String dbName : dbNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(dbName, null, null, new String[] {"TABLE"}),
                                    "TABLE_NAME");
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(dbName + "." + tbName));
                }
            } else {
                List<String> dbNames = getResultList(metaData.getSchemas(), "TABLE_SCHEM");
                dbNames =
                        dbNames.stream()
                                .filter(dbName -> Pattern.matches(dbPattern, dbName))
                                .collect(Collectors.toList());
                for (String dbName : dbNames) {
                    List<String> tableNames =
                            getResultList(
                                    metaData.getTables(null, dbName, null, new String[] {"TABLE"}),
                                    "TABLE_NAME");
                    tableNames.stream()
                            .filter(tbName -> Pattern.matches(tbPattern, tbName))
                            .forEach(tbName -> result.add(dbName + "." + tbName));
                }
            }
            return result;
        }
    }

    private static List<String> getResultList(ResultSet resultSet, String columnName)
            throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString(columnName));
        }
        return result;
    }
}
