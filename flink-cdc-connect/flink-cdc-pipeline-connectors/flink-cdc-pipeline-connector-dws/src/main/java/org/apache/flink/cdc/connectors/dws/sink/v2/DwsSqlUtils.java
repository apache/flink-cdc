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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.types.DataTypeChecks.getLength;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getPrecision;
import static org.apache.flink.cdc.common.types.DataTypeChecks.getScale;

/** SQL helpers for the DWS SinkV2 staging-table commit path. */
public final class DwsSqlUtils {

    static final String OP_COLUMN = "__flink_cdc_op";
    static final String SEQUENCE_COLUMN = "__flink_cdc_seq";
    static final String ROW_NUMBER_COLUMN = "__flink_cdc_rn";
    static final String UPSERT_OPERATION = "UPSERT";
    static final String DELETE_OPERATION = "DELETE";
    static final String COMMIT_TABLE = "flink_cdc_commits";

    private static final int MAX_IDENTIFIER_LENGTH = 63;

    private DwsSqlUtils() {}

    public static String normalizeIdentifier(String identifier, boolean caseSensitive) {
        String value = identifier.trim();
        return caseSensitive ? value : value.toLowerCase(Locale.ROOT);
    }

    public static String normalizeSchemaName(
            TableId tableId, String defaultSchema, boolean caseSensitive) {
        String schemaName = tableId.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            schemaName = defaultSchema;
        }
        return normalizeIdentifier(schemaName, caseSensitive);
    }

    public static String normalizeTableName(TableId tableId, boolean caseSensitive) {
        return normalizeIdentifier(tableId.getTableName(), caseSensitive);
    }

    public static String quoteIdentifier(String identifier, boolean caseSensitive) {
        String normalized = normalizeIdentifier(identifier, caseSensitive);
        return '"' + normalized.replace("\"", "\"\"") + '"';
    }

    public static String formatTableIdentifier(
            String schemaName, String tableName, boolean caseSensitive) {
        return quoteIdentifier(schemaName, caseSensitive)
                + "."
                + quoteIdentifier(tableName, caseSensitive);
    }

    public static String buildStagingTableName(
            String schemaName,
            String tableName,
            String jobId,
            long checkpointId,
            int subtaskId,
            int stagingSequence,
            boolean caseSensitive) {
        String seed = schemaName + "." + tableName + "." + jobId;
        String hash = Integer.toHexString(seed.hashCode() & 0x7fffffff);
        String name =
                "flink_cdc_stage_"
                        + hash
                        + "_"
                        + checkpointId
                        + "_"
                        + subtaskId
                        + "_"
                        + stagingSequence;
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            name =
                    "fcdc_stage_"
                            + hash
                            + "_"
                            + Long.toHexString(checkpointId)
                            + "_"
                            + subtaskId
                            + "_"
                            + stagingSequence;
        }
        return normalizeIdentifier(name, caseSensitive);
    }

    public static String buildCreateStagingTableSql(
            String stagingSchema, String stagingTable, Schema schema, boolean caseSensitive) {
        List<String> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(quoteIdentifier(OP_COLUMN, caseSensitive) + " VARCHAR(16) NOT NULL");
        columnDefinitions.add(quoteIdentifier(SEQUENCE_COLUMN, caseSensitive) + " BIGINT NOT NULL");
        for (Column column : schema.getColumns()) {
            columnDefinitions.add(
                    quoteIdentifier(column.getName(), caseSensitive)
                            + " "
                            + toDwsType(column.getType()));
        }
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s)",
                formatTableIdentifier(stagingSchema, stagingTable, caseSensitive),
                String.join(", ", columnDefinitions));
    }

    public static String buildInsertStagingSql(
            String stagingSchema,
            String stagingTable,
            List<String> columnNames,
            boolean caseSensitive) {
        List<String> columns = new ArrayList<>();
        columns.add(quoteIdentifier(OP_COLUMN, caseSensitive));
        columns.add(quoteIdentifier(SEQUENCE_COLUMN, caseSensitive));
        for (String columnName : columnNames) {
            columns.add(quoteIdentifier(columnName, caseSensitive));
        }

        List<String> placeholders = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            placeholders.add("?");
        }

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                formatTableIdentifier(stagingSchema, stagingTable, caseSensitive),
                String.join(", ", columns),
                String.join(", ", placeholders));
    }

    public static String buildCreateCommitTableSql(String schemaName, boolean caseSensitive) {
        String commitTable = formatTableIdentifier(schemaName, COMMIT_TABLE, caseSensitive);
        return String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(128) NOT NULL, "
                        + "%s BIGINT NOT NULL, %s INTEGER NOT NULL, "
                        + "%s VARCHAR(512) NOT NULL, %s VARCHAR(512) NOT NULL, "
                        + "%s TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (%s, %s, %s, %s, %s))",
                commitTable,
                quoteIdentifier("job_id", caseSensitive),
                quoteIdentifier("checkpoint_id", caseSensitive),
                quoteIdentifier("subtask_id", caseSensitive),
                quoteIdentifier("target_table", caseSensitive),
                quoteIdentifier("staging_table", caseSensitive),
                quoteIdentifier("committed_at", caseSensitive),
                quoteIdentifier("job_id", caseSensitive),
                quoteIdentifier("checkpoint_id", caseSensitive),
                quoteIdentifier("subtask_id", caseSensitive),
                quoteIdentifier("target_table", caseSensitive),
                quoteIdentifier("staging_table", caseSensitive));
    }

    public static String buildSelectCommitMarkerSql(String schemaName, boolean caseSensitive) {
        return String.format(
                "SELECT 1 FROM %s WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? LIMIT 1",
                formatTableIdentifier(schemaName, COMMIT_TABLE, caseSensitive),
                quoteIdentifier("job_id", caseSensitive),
                quoteIdentifier("checkpoint_id", caseSensitive),
                quoteIdentifier("subtask_id", caseSensitive),
                quoteIdentifier("target_table", caseSensitive),
                quoteIdentifier("staging_table", caseSensitive));
    }

    public static String buildInsertCommitMarkerSql(String schemaName, boolean caseSensitive) {
        List<String> columns = new ArrayList<>();
        columns.add(quoteIdentifier("job_id", caseSensitive));
        columns.add(quoteIdentifier("checkpoint_id", caseSensitive));
        columns.add(quoteIdentifier("subtask_id", caseSensitive));
        columns.add(quoteIdentifier("target_table", caseSensitive));
        columns.add(quoteIdentifier("staging_table", caseSensitive));
        return String.format(
                "INSERT INTO %s (%s) VALUES (?, ?, ?, ?, ?)",
                formatTableIdentifier(schemaName, COMMIT_TABLE, caseSensitive),
                String.join(", ", columns));
    }

    public static String buildDeleteLatestRowsSql(
            DwsCommittable committable, boolean caseSensitive) {
        return String.format(
                "DELETE FROM %s AS t USING (%s) AS s WHERE %s",
                formatTableIdentifier(
                        committable.getTargetSchema(), committable.getTargetTable(), caseSensitive),
                buildLatestRowsQuery(committable, caseSensitive),
                buildPrimaryKeyJoinCondition(
                        committable.getPrimaryKeys(), "t", "s", caseSensitive));
    }

    public static String buildInsertLatestRowsSql(
            DwsCommittable committable, boolean caseSensitive) {
        List<String> quotedColumns =
                committable.getColumnNames().stream()
                        .map(columnName -> quoteIdentifier(columnName, caseSensitive))
                        .collect(Collectors.toList());
        List<String> selectedColumns =
                committable.getColumnNames().stream()
                        .map(columnName -> "s." + quoteIdentifier(columnName, caseSensitive))
                        .collect(Collectors.toList());
        return String.format(
                "INSERT INTO %s (%s) SELECT %s FROM (%s) AS s WHERE s.%s <> '%s'",
                formatTableIdentifier(
                        committable.getTargetSchema(), committable.getTargetTable(), caseSensitive),
                String.join(", ", quotedColumns),
                String.join(", ", selectedColumns),
                buildLatestRowsQuery(committable, caseSensitive),
                quoteIdentifier(OP_COLUMN, caseSensitive),
                DELETE_OPERATION);
    }

    public static String buildDropTableSql(
            String schemaName, String tableName, boolean caseSensitive) {
        return "DROP TABLE IF EXISTS "
                + formatTableIdentifier(schemaName, tableName, caseSensitive);
    }

    private static String buildLatestRowsQuery(DwsCommittable committable, boolean caseSensitive) {
        String partitionColumns =
                committable.getPrimaryKeys().stream()
                        .map(primaryKey -> "s." + quoteIdentifier(primaryKey, caseSensitive))
                        .collect(Collectors.joining(", "));
        return String.format(
                "SELECT * FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY s.%s DESC) "
                        + "AS %s FROM %s AS s) AS ranked WHERE ranked.%s = 1",
                partitionColumns,
                quoteIdentifier(SEQUENCE_COLUMN, caseSensitive),
                quoteIdentifier(ROW_NUMBER_COLUMN, caseSensitive),
                formatTableIdentifier(
                        committable.getStagingSchema(),
                        committable.getStagingTable(),
                        caseSensitive),
                quoteIdentifier(ROW_NUMBER_COLUMN, caseSensitive));
    }

    private static String buildPrimaryKeyJoinCondition(
            List<String> primaryKeys, String leftAlias, String rightAlias, boolean caseSensitive) {
        return primaryKeys.stream()
                .map(
                        primaryKey ->
                                leftAlias
                                        + "."
                                        + quoteIdentifier(primaryKey, caseSensitive)
                                        + " = "
                                        + rightAlias
                                        + "."
                                        + quoteIdentifier(primaryKey, caseSensitive))
                .collect(Collectors.joining(" AND "));
    }

    private static String toDwsType(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                return String.format("DECIMAL(%d, %d)", getPrecision(type), getScale(type));
            case CHAR:
                return String.format("CHAR(%d)", getLength(type));
            case VARCHAR:
                int length = getLength(type);
                return length >= 10_485_760 ? "TEXT" : String.format("VARCHAR(%d)", length);
            case BINARY:
            case VARBINARY:
                return "BYTEA";
            case DATE:
                return "DATE";
            case TIME_WITHOUT_TIME_ZONE:
                return String.format("TIME(%d)", normalizePrecision(getPrecision(type)));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return String.format("TIMESTAMP(%d)", normalizePrecision(getPrecision(type)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return String.format("TIMESTAMPTZ(%d)", normalizePrecision(getPrecision(type)));
            case ARRAY:
                return "TEXT";
            case MAP:
            case ROW:
                return "JSON";
            default:
                throw new IllegalArgumentException(
                        "Unsupported data type for GaussDB DWS DDL: " + type);
        }
    }

    private static int normalizePrecision(int precision) {
        return Math.max(0, Math.min(6, precision));
    }
}
