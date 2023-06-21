/*
 * Copyright 2023 Ververica Inc.
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

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;

import java.util.List;

/** OceanBase dialect for Mysql mode. */
public class OceanBaseMysqlDialect extends OceanBaseDialect {

    private static final long serialVersionUID = 1;

    @Override
    public String quoteIdentifier(@Nonnull String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String getQueryPrimaryKeySql(@Nonnull String dbName, @Nonnull String tableName) {
        return String.format(
                "SELECT COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND upper(INDEX_NAME) = 'PRIMARY'",
                dbName, tableName);
    }

    @Override
    public String getQueryNewChunkBoundSql(
            @Nonnull String dbName,
            @Nonnull String tableName,
            List<String> chunkKeyColumns,
            List<Object> oldChunkBound,
            Integer chunkSize) {
        String fullTableName = getFullTableName(dbName, tableName);
        String whereClause;
        String limitClause;
        if (oldChunkBound == null) {
            limitClause = "LIMIT 1";
            whereClause = "";
        } else {
            limitClause = String.format("LIMIT %d,1", chunkSize - 1);
            whereClause = "WHERE " + getConditionGreat(chunkKeyColumns, oldChunkBound);
        }
        String selectFields = StringUtils.join(chunkKeyColumns, ",");
        return String.format(
                "SELECT %s FROM %s %s ORDER BY %s ASC %s",
                selectFields, fullTableName, whereClause, selectFields, limitClause);
    }
}
