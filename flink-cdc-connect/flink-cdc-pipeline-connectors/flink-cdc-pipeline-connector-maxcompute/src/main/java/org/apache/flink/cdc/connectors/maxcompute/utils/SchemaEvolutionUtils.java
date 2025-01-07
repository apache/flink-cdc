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

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.util.CollectionUtil;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.task.SQLTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Schema evolution utils for maxcompute. */
public class SchemaEvolutionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionUtils.class);
    private static final Map<String, String> unsupportSchemahints = new HashMap<>();
    private static final Map<String, String> supportSchemaHints = new HashMap<>();

    static {
        unsupportSchemahints.put("odps.sql.type.system.odps2", "true");
        unsupportSchemahints.put("odps.sql.decimal.odps2", "true");
        unsupportSchemahints.put("odps.sql.allow.schema.evolution", "true");

        supportSchemaHints.put("odps.sql.type.system.odps2", "true");
        supportSchemaHints.put("odps.sql.decimal.odps2", "true");
        supportSchemaHints.put("odps.namespace.schema", "true");
        supportSchemaHints.put("odps.sql.allow.namespace.schema", "true");
        supportSchemaHints.put("odps.sql.allow.schema.evolution", "true");
    }

    private SchemaEvolutionUtils() {}

    /**
     * equals to run a sql like: create table table_name (col_name1 type1 comment [, col_name2 type2
     * ...]);.
     */
    public static void createTable(MaxComputeOptions options, TableId tableId, Schema schema)
            throws OdpsException {
        Odps odps = MaxComputeUtils.getOdps(options);
        TableSchema tableSchema = TypeConvertUtils.toMaxCompute(schema);
        if (options.isSupportSchema()
                && !StringUtils.isNullOrWhitespaceOnly(tableId.getNamespace())) {
            LOG.info("create schema {}", tableId.getNamespace());
            odps.schemas()
                    .create(
                            odps.getDefaultProject(),
                            tableId.getNamespace(),
                            "generate by Flink CDC",
                            true);
        }
        Tables.TableCreator tableCreator =
                odps.tables()
                        .newTableCreator(
                                odps.getDefaultProject(), tableId.getTableName(), tableSchema)
                        .withHints(unsupportSchemahints)
                        .ifNotExists()
                        .debug();
        if (!CollectionUtil.isNullOrEmpty(schema.primaryKeys())) {
            tableCreator
                    .transactionTable()
                    .withBucketNum(options.getBucketsNum())
                    .withPrimaryKeys(schema.primaryKeys());
        }
        if (options.isSupportSchema()) {
            if (StringUtils.isNullOrWhitespaceOnly(tableId.getNamespace())) {
                tableCreator.withSchemaName("default").withHints(supportSchemaHints);
            } else {
                tableCreator.withSchemaName(tableId.getNamespace()).withHints(supportSchemaHints);
            }
        }
        LOG.info("create table {}, schema {}", getFullTableName(options, tableId), schema);
        tableCreator.create();
    }

    /**
     * equals to run a sql like: 'alter table table_name add columns (col_name1 type1 comment [,
     * col_name2 type2 ...]);'.
     */
    public static void addColumns(
            MaxComputeOptions options,
            TableId tableId,
            List<AddColumnEvent.ColumnWithPosition> columns)
            throws OdpsException {
        Odps odps = MaxComputeUtils.getOdps(options);

        StringBuilder sqlBuilder =
                new StringBuilder(
                        "alter table " + getFullTableName(options, tableId) + " add columns (");

        for (AddColumnEvent.ColumnWithPosition addColumn : columns) {
            if (addColumn.getPosition() == AddColumnEvent.ColumnPosition.LAST) {
                sqlBuilder
                        .append(addColumn.getAddColumn().getName())
                        .append(" ")
                        .append(string(addColumn.getAddColumn().getType()))
                        .append(" comment '")
                        .append(addColumn.getAddColumn().getType().asSummaryString())
                        .append("',");
            } else {
                throw new UnsupportedOperationException(
                        "Not support position: "
                                + addColumn.getPosition()
                                + " "
                                + addColumn.getExistedColumnName());
            }
        }
        // remove ','
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(");");

        Instance instance =
                SQLTask.run(
                        odps,
                        odps.getDefaultProject(),
                        sqlBuilder.toString(),
                        options.isSupportSchema() ? supportSchemaHints : unsupportSchemahints,
                        null);
        LOG.info("execute add column task: `{}`, instanceId: {}", sqlBuilder, instance.getId());
        instance.waitForSuccess();
    }

    /**
     * equals to run a sql like: 'alter table table_name change column old_column_name
     * new_column_name new_data_type;'. and 'alter table table_name change column col_name comment
     * 'col_comment'';
     */
    public static void alterColumnType(
            MaxComputeOptions options, TableId tableId, Map<String, DataType> typeMapping)
            throws OdpsException {
        Odps odps = MaxComputeUtils.getOdps(options);

        String prefix = "alter table " + getFullTableName(options, tableId) + " change column ";

        for (Map.Entry<String, DataType> entry : typeMapping.entrySet()) {
            String alterColumnSql =
                    prefix
                            + entry.getKey()
                            + " "
                            + entry.getKey()
                            + " "
                            + string(entry.getValue())
                            + ";";
            Instance instance =
                    SQLTask.run(
                            odps,
                            odps.getDefaultProject(),
                            alterColumnSql,
                            options.isSupportSchema() ? supportSchemaHints : unsupportSchemahints,
                            null);
            LOG.info(
                    "execute alter column task: `{}`, instanceId: {}",
                    alterColumnSql,
                    instance.getId());
            instance.waitForSuccess();
        }
    }

    /**
     * equals to run a sql like: 'alter table table_name drop columns col_name1[, col_name2...];'.
     */
    public static void dropColumn(
            MaxComputeOptions options, TableId tableId, List<String> droppedColumnNames)
            throws OdpsException {
        Odps odps = MaxComputeUtils.getOdps(options);
        StringBuilder sqlBuilder =
                new StringBuilder(
                        "alter table " + getFullTableName(options, tableId) + " drop columns ");
        for (String column : droppedColumnNames) {
            sqlBuilder.append(column).append(",");
        }
        // remove ','
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(";");
        Instance instance =
                SQLTask.run(
                        odps,
                        odps.getDefaultProject(),
                        sqlBuilder.toString(),
                        options.isSupportSchema() ? supportSchemaHints : unsupportSchemahints,
                        null);
        LOG.info("execute drop column task: `{}`, instanceId: {}", sqlBuilder, instance.getId());
        instance.waitForSuccess();
    }

    /**
     * equals to run a sql like: 'alter table table_name change column old_col_name rename to
     * new_col_name;'.
     */
    public static void renameColumn(
            MaxComputeOptions options, TableId tableId, Map<String, String> nameMapping)
            throws OdpsException {
        Odps odps = MaxComputeUtils.getOdps(options);
        String prefix = "alter table " + getFullTableName(options, tableId) + " change column ";
        for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
            String sql = prefix + entry.getKey() + " rename to " + entry.getValue() + ";";
            Instance instance =
                    SQLTask.run(
                            odps,
                            odps.getDefaultProject(),
                            sql,
                            options.isSupportSchema() ? supportSchemaHints : unsupportSchemahints,
                            null);
            LOG.info("execute rename column task: `{}`, instanceId: {}", sql, instance.getId());
            instance.waitForSuccess();
        }
    }

    public static void dropTable(MaxComputeOptions options, TableId tableId) throws OdpsException {
        Odps odps = MaxComputeUtils.getOdps(options);
        Table table = MaxComputeUtils.getTable(options, tableId);
        odps.tables().delete(table.getProject(), table.getSchemaName(), table.getName(), false);
    }

    public static void truncateTable(MaxComputeOptions options, TableId tableId)
            throws OdpsException {
        Table table = MaxComputeUtils.getTable(options, tableId);
        table.truncate();
    }

    private static String getFullTableName(MaxComputeOptions options, TableId tableId) {
        if (options.isSupportSchema()) {
            if (StringUtils.isNullOrWhitespaceOnly(tableId.getNamespace())) {
                return "`" + options.getProject() + "`.`default`.`" + tableId.getTableName() + "`";
            } else {
                return "`"
                        + options.getProject()
                        + "`.`"
                        + tableId.getNamespace()
                        + "`.`"
                        + tableId.getTableName()
                        + "`";
            }
        } else {
            return "`" + options.getProject() + "`.`" + tableId.getTableName() + "`";
        }
    }

    private static String string(DataType dataType) {
        return TypeConvertUtils.toMaxCompute(dataType).getTypeName();
    }
}
