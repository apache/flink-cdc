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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** A utility class for table discovery. */
public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    public static List<TableId> listTables(
            String database,
            JdbcConnection jdbc,
            RelationalTableFilters tableFilters,
            boolean includePartitionedTables)
            throws SQLException {

        String[] tableTypes = new String[] {"TABLE"};
        if (includePartitionedTables) {
            tableTypes = new String[] {"TABLE", "PARTITIONED TABLE"};
        }
        Set<TableId> allTableIds = jdbc.readTableNames(database, null, null, tableTypes);

        Set<TableId> capturedTables =
                allTableIds.stream()
                        .filter(t -> tableFilters.dataCollectionFilter().isIncluded(t))
                        .collect(Collectors.toSet());
        LOG.info(
                "Postgres captured tables : {} .",
                capturedTables.stream().map(TableId::toString).collect(Collectors.joining(",")));

        return new ArrayList<>(capturedTables);
    }
}
