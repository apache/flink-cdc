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

package org.apache.flink.cdc.connectors.tidb.source.schema;

import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.util.CollectionUtil;

import io.debezium.relational.TableId;

import java.util.List;
import java.util.stream.Collectors;

public class TiDBTableDefinition {
    TableId tableId;
    List<TiDBFieldDefinition> fieldDefinitions;
    List<String> primaryKeys;

    public TiDBTableDefinition(
            TableId tableId, List<TiDBFieldDefinition> fieldDefinitions, List<String> primaryKeys) {
        this.tableId = tableId;
        this.fieldDefinitions = fieldDefinitions;
        this.primaryKeys = primaryKeys;
    }

    public String toDdl() {
        return String.format(
                "CREATE TABLE %s (\n\t %s %s );",
                TiDBUtils.quote(tableId), fieldDefinitions(), pkDefinition());
    }

    private String fieldDefinitions() {
        return fieldDefinitions.stream()
                .map(TiDBFieldDefinition::toDdl)
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
                                    .map(TiDBUtils::quote)
                                    .collect(Collectors.joining(","))));
        }
        return pkDefinition.toString();
    }
}
