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

import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** OceanBase Snapshot Chunk. */
public class OceanBaseSnapshotChunkReader {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSnapshotChunkReader.class);

    private final OceanBaseDialect dialect;
    private final String dbName;
    private final String tableName;

    private final List<String> chunkKeyColumns;
    private final List<Object> lowerBound;
    private final List<Object> upperBound;
    private final int chunkSize;
    private final JdbcConnection.ResultSetConsumer resultSetConsumer;

    public OceanBaseSnapshotChunkReader(
            OceanBaseDialect dialect,
            String dbName,
            String tableName,
            List<String> chunkKeyColumns,
            List<Object> lowerBound,
            List<Object> upperBound,
            int chunkSize,
            JdbcConnection.ResultSetConsumer resultSetConsumer) {
        this.dialect = dialect;
        this.dbName = dbName;
        this.tableName = tableName;
        this.chunkKeyColumns = chunkKeyColumns;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.chunkSize = chunkSize;
        this.resultSetConsumer = resultSetConsumer;
    }

    public void read(OceanBaseDataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.setFetchSize(chunkSize);
            String sql =
                    dialect.getQueryChunkSql(
                            dbName, tableName, chunkKeyColumns, lowerBound, upperBound);
            LOG.debug("Query chunk data sql: " + sql);
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.accept(resultSet);
        }
    }
}
