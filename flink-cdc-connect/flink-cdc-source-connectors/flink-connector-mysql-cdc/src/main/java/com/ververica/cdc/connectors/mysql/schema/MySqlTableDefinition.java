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

package com.ververica.cdc.connectors.mysql.schema;

import org.apache.flink.util.CollectionUtil;

import com.ververica.cdc.connectors.mysql.source.utils.StatementUtils;
import io.debezium.relational.TableId;

import java.util.List;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.utils.StatementUtils.quote;

/** used to generate table definition in ddl with "desc table". */
public class MySqlTableDefinition {
    TableId tableId;
    List<MySqlFieldDefinition> fieldDefinitions;
    List<String> primaryKeys;

    public MySqlTableDefinition(
            TableId tableId,
            List<MySqlFieldDefinition> fieldDefinitions,
            List<String> primaryKeys) {
        this.tableId = tableId;
        this.fieldDefinitions = fieldDefinitions;
        this.primaryKeys = primaryKeys;
    }

    public String toDdl() {
        return String.format(
                "CREATE TABLE %s (\n\t %s %s );",
                quote(tableId), fieldDefinitions(), pkDefinition());
    }

    private String fieldDefinitions() {
        return fieldDefinitions.stream()
                .map(MySqlFieldDefinition::toDdl)
                .collect(Collectors.joining(", \n\t"));
    }

    private String pkDefinition() {
        StringBuilder pkDefinition = new StringBuilder();
        if (!CollectionUtil.isNullOrEmpty(primaryKeys)) {
            pkDefinition.append(",");
            pkDefinition.append(
                    String.format(
                            "PRIMARY KEY ( %s )",
                            primaryKeys.stream()
                                    .map(StatementUtils::quote)
                                    .collect(Collectors.joining(","))));
        }
        return pkDefinition.toString();
    }
}
