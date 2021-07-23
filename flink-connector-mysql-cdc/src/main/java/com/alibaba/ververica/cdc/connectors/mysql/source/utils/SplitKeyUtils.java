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

package com.alibaba.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.debezium.relational.Table;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utils to obtain and validate split key of table. */
public class SplitKeyUtils {

    private SplitKeyUtils() {}

    /**
     * Returns the split key type, the split key should be single field。
     *
     * <p>The split key is primary key when primary key contains single field, the split key will be
     * inferred when the primary key contains multiple field.
     *
     * @param definedPkType the defined primary key type in Flink.
     * @param actualTable the table schema in MySQL database.
     */
    public static RowType validateAndGetSplitKeyType(RowType definedPkType, Table actualTable) {
        Preconditions.checkState(
                definedPkType.getFieldCount() >= 1,
                "The primary key is required in Flink SQL Table definition.");
        Preconditions.checkState(
                !actualTable.primaryKeyColumnNames().isEmpty(),
                String.format(
                        "Only supports capture table with primary key, but the table %s has no primary key.",
                        actualTable.id()));

        validatePrimaryKey(definedPkType.getFieldNames(), actualTable.primaryKeyColumnNames());

        if (definedPkType.getFieldCount() == 1) {
            return definedPkType;
        } else {
            // use the first defined primary key used combine key.
            return new RowType(Arrays.asList(definedPkType.getFields().get(0)));
        }
    }

    public static boolean splitKeyIsAutoIncremented(RowType splitKeyType, Table actualTable) {
        final String splitKeyName = unquoteColumnName(splitKeyType.getFieldNames().get(0));
        return !actualTable.primaryKeyColumnNames().isEmpty()
                && actualTable.isAutoIncremented(splitKeyName);
    }

    private static void validatePrimaryKey(
            List<String> definedPkFieldnames, List<String> actualPkFieldNames) {
        List<String> formattedDefinedPk =
                definedPkFieldnames.stream()
                        .map(SplitKeyUtils::unquoteColumnName)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> formattedActualPk =
                actualPkFieldNames.stream()
                        .map(SplitKeyUtils::unquoteColumnName)
                        .sorted()
                        .collect(Collectors.toList());

        String exceptionMsg =
                String.format(
                        "The defined primary key %s in Flink is not matched with actual primary key %s in MySQL",
                        definedPkFieldnames, actualPkFieldNames);
        Preconditions.checkState(
                formattedDefinedPk.size() == formattedActualPk.size()
                        && formattedDefinedPk.containsAll(formattedActualPk),
                exceptionMsg);
    }

    public static String unquoteColumnName(String columnName) {
        if (columnName == null) {
            return null;
        }
        if (columnName.length() < 2) {
            return columnName.toLowerCase();
        }

        Character quotingChar = deriveQuotingChar(columnName);
        if (quotingChar != null) {
            columnName = columnName.substring(1, columnName.length() - 1);
            columnName =
                    columnName.replace(
                            quotingChar.toString() + quotingChar.toString(),
                            quotingChar.toString());
        }
        return columnName.toLowerCase();
    }

    private static Character deriveQuotingChar(String columnName) {
        char first = columnName.charAt(0);
        char last = columnName.charAt(columnName.length() - 1);

        if (first == last && (first == '"' || first == '\'' || first == '`')) {
            return first;
        }

        return null;
    }
}
