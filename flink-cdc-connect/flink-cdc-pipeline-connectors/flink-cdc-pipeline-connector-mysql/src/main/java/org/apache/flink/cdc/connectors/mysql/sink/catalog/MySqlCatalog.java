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

package org.apache.flink.cdc.connectors.mysql.sink.catalog;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.conn.JdbcConnectionPoolFactory;

import java.util.List;

/** MySQL catalog. */
public class MySqlCatalog extends AbstractJdbcCatalog {
    public MySqlCatalog(
            String name, JdbcSinkConfig sinkConfig, JdbcConnectionPoolFactory poolFactory) {
        super(name, sinkConfig, poolFactory);
    }

    @Override
    protected String buildUpsertSql(TableId tableId, Schema schema) {
        return MySqlSmtCreatorFactory.INSTANCE.buildUpsertSql(tableId, schema.getColumns());
    }

    @Override
    protected String buildCreateTableSql(TableId tableId, Schema schema, boolean ignoreIfExists) {
        return MySqlSmtCreatorFactory.INSTANCE.buildCreateTableSql(tableId, schema, ignoreIfExists);
    }

    @Override
    protected String buildAlterAddColumnsSql(
            TableId tableId, List<AddColumnEvent.ColumnWithPosition> addColumnEvent) {
        return MySqlSmtCreatorFactory.INSTANCE.buildAlterAddColumnsSql(tableId, addColumnEvent);
    }

    @Override
    protected String buildRenameColumnSql(TableId tableId, String oldName, String newName) {
        return MySqlSmtCreatorFactory.INSTANCE.buildRenameColumnSql(tableId, oldName, newName);
    }

    @Override
    protected String buildDropColumnSql(TableId tableId, String column) {
        return MySqlSmtCreatorFactory.INSTANCE.buildDropColumnSql(tableId, column);
    }

    @Override
    protected String buildAlterColumnTypeSql(
            TableId tableId, String columnName, DataType columnType) {
        return MySqlSmtCreatorFactory.INSTANCE.buildAlterColumnTypeSql(
                tableId, columnName, columnType);
    }
}
