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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/** Commits DWS staging tables into target tables in an idempotent transaction. */
public class DwsCommitter implements Committer<DwsCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DwsCommitter.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String defaultSchema;
    private final boolean caseSensitive;

    private transient Connection connection;

    public DwsCommitter(
            String jdbcUrl,
            String username,
            String password,
            String defaultSchema,
            boolean caseSensitive) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.defaultSchema = defaultSchema;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public void commit(Collection<CommitRequest<DwsCommittable>> commitRequests)
            throws IOException {
        if (commitRequests.isEmpty()) {
            return;
        }

        for (CommitRequest<DwsCommittable> request : commitRequests) {
            DwsCommittable committable = request.getCommittable();
            try {
                commitOne(committable);
                request.signalAlreadyCommitted();
            } catch (Exception e) {
                rollbackQuietly();
                request.retryLater();
                LOG.warn(
                        "Failed to commit DWS staging table {}.{} for checkpoint {}.",
                        committable.getStagingSchema(),
                        committable.getStagingTable(),
                        committable.getCheckpointId(),
                        e);
            }
        }
    }

    void commitCommittables(Collection<DwsCommittable> committables) throws IOException {
        for (DwsCommittable committable : committables) {
            try {
                commitOne(committable);
            } catch (Exception e) {
                rollbackQuietly();
                throw new IOException(
                        String.format(
                                "Failed to commit DWS staging table %s.%s for checkpoint %s.",
                                committable.getStagingSchema(),
                                committable.getStagingTable(),
                                committable.getCheckpointId()),
                        e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    private void commitOne(DwsCommittable committable) throws SQLException {
        if (committable.getPrimaryKeys().isEmpty()) {
            throw new SQLException(
                    "DWS application-level two-phase commit requires primary keys for "
                            + committable.getTargetIdentifier());
        }

        Connection jdbcConnection = getConnection();
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(DwsSqlUtils.buildCreateCommitTableSql(defaultSchema, caseSensitive));

            if (isAlreadyCommitted(jdbcConnection, committable)) {
                dropStagingTable(statement, committable);
                jdbcConnection.commit();
                LOG.info(
                        "Skipped already committed DWS staging table {}.{} for checkpoint {}.",
                        committable.getStagingSchema(),
                        committable.getStagingTable(),
                        committable.getCheckpointId());
                return;
            }

            statement.execute(DwsSqlUtils.buildDeleteLatestRowsSql(committable, caseSensitive));
            statement.execute(DwsSqlUtils.buildInsertLatestRowsSql(committable, caseSensitive));
            insertCommitMarker(jdbcConnection, committable);
            dropStagingTable(statement, committable);
            jdbcConnection.commit();
            LOG.info(
                    "Committed DWS staging table {}.{} into {} for checkpoint {}.",
                    committable.getStagingSchema(),
                    committable.getStagingTable(),
                    committable.getTargetIdentifier(),
                    committable.getCheckpointId());
        }
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(false);
        }
        return connection;
    }

    private boolean isAlreadyCommitted(Connection jdbcConnection, DwsCommittable committable)
            throws SQLException {
        try (PreparedStatement statement =
                jdbcConnection.prepareStatement(
                        DwsSqlUtils.buildSelectCommitMarkerSql(defaultSchema, caseSensitive))) {
            statement.setString(1, committable.getJobId());
            statement.setLong(2, committable.getCheckpointId());
            statement.setInt(3, committable.getSubtaskId());
            statement.setString(4, committable.getTargetIdentifier());
            statement.setString(
                    5, committable.getStagingSchema() + "." + committable.getStagingTable());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private void insertCommitMarker(Connection jdbcConnection, DwsCommittable committable)
            throws SQLException {
        try (PreparedStatement statement =
                jdbcConnection.prepareStatement(
                        DwsSqlUtils.buildInsertCommitMarkerSql(defaultSchema, caseSensitive))) {
            statement.setString(1, committable.getJobId());
            statement.setLong(2, committable.getCheckpointId());
            statement.setInt(3, committable.getSubtaskId());
            statement.setString(4, committable.getTargetIdentifier());
            statement.setString(
                    5, committable.getStagingSchema() + "." + committable.getStagingTable());
            statement.executeUpdate();
        }
    }

    private void dropStagingTable(Statement statement, DwsCommittable committable)
            throws SQLException {
        statement.execute(
                DwsSqlUtils.buildDropTableSql(
                        committable.getStagingSchema(),
                        committable.getStagingTable(),
                        caseSensitive));
    }

    private void rollbackQuietly() {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOG.warn("Failed to rollback DWS commit transaction.", e);
        }
    }
}
