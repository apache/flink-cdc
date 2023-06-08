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

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.List;

/** OceanBase dialect. */
public abstract class OceanBaseDialect implements Serializable {

    public abstract String quoteIdentifier(@Nonnull String identifier);

    public abstract String getQueryPrimaryKeySql(@Nonnull String dbName, @Nonnull String tableName);

    public abstract String getQueryNewChunkBoundSql(
            @Nonnull String dbName,
            @Nonnull String tableName,
            List<String> chunkKeyColumns,
            List<Object> oldChunkBound,
            Integer chunkSize);

    String getFullTableName(String dbName, String tableName) {
        return String.format("%s.%s", quoteIdentifier(dbName), quoteIdentifier(tableName));
    }

    public String getQueryChunkSql(
            String dbName,
            String tableName,
            List<String> chunkKeyColumns,
            List<Object> lowerBound,
            List<Object> upperBound) {
        String fullTableName = getFullTableName(dbName, tableName);
        String whereClause;
        if (lowerBound == null) {
            if (upperBound == null) {
                whereClause = "";
            } else {
                whereClause = "WHERE " + getConditionLessOrEqual(chunkKeyColumns, upperBound);
            }
        } else {
            if (upperBound == null) {
                whereClause = "WHERE " + getConditionGreat(chunkKeyColumns, lowerBound);
            } else {
                whereClause =
                        String.format(
                                "WHERE (%s) AND (%s)",
                                getConditionGreat(chunkKeyColumns, lowerBound),
                                getConditionLessOrEqual(chunkKeyColumns, upperBound));
            }
        }
        return String.format("SELECT * FROM %s %s", fullTableName, whereClause);
    }

    String getConditionLessOrEqual(List<String> fieldNames, List<Object> values) {
        StringBuilder cond = new StringBuilder();
        cond.append("((");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (0 != i) {
                cond.append(" or (");
            }
            for (int j = 0; j < i; j++) {
                cond.append(fieldNames.get(j));
                cond.append(" = ");
                cond.append(values.get(j));
                cond.append(" and ");
            }
            cond.append(fieldNames.get(i));
            cond.append(i == fieldNames.size() - 1 ? " <= " : " < ");
            cond.append(values.get(i));
            cond.append(")");
        }
        return cond.append(")").toString();
    }

    String getConditionGreat(List<String> fieldNames, List<Object> values) {
        StringBuilder cond = new StringBuilder();
        cond.append("((");
        for (int i = 0; i < fieldNames.size(); ++i) {
            if (0 != i) {
                cond.append(" or (");
            }
            for (int j = 0; j < i; ++j) {
                cond.append(fieldNames.get(j));
                cond.append(" = ");
                cond.append(values.get(j));
                cond.append(" and ");
            }
            cond.append(fieldNames.get(i));
            cond.append(" > ");
            cond.append(values.get(i));
            cond.append(")");
        }
        return cond.append(")").toString();
    }
}
