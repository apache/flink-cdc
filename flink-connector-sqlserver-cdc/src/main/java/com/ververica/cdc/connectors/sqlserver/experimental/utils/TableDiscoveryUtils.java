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

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Utilities to discovery matched tables. */
public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    private static final String GET_DATABASE_NAME = "SELECT db_name()";

    private static final String GET_LIST_OF_CDC_ENABLED_TABLES =
            "EXEC sys.sp_cdc_help_change_data_capture";

    public static List<TableId> listTables(JdbcConnection jdbc, RelationalTableFilters tableFilters)
            throws SQLException {

        String realDatabaseName =
                jdbc.queryAndMap(
                        GET_DATABASE_NAME,
                        jdbc.singleResultMapper(
                                rs -> rs.getString(1), "Could not retrieve database name"));

        return jdbc.queryAndMap(
                GET_LIST_OF_CDC_ENABLED_TABLES,
                rs -> {
                    final List<TableId> capturedTableIds = new ArrayList<>();
                    while (rs.next()) {
                        TableId tableId =
                                new TableId(realDatabaseName, rs.getString(1), rs.getString(2));
                        if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                            capturedTableIds.add(tableId);
                            LOG.info("\t including '{}' for further processing", tableId);
                        } else {
                            LOG.info("\t '{}' is filtered out of capturing", tableId);
                        }
                    }
                    return capturedTableIds;
                });
    }
}
