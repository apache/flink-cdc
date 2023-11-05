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

package com.ververica.cdc.common.event;

import java.util.Objects;

/** Unique identifier for a database table. */
public class TableId {

    public static final String DEFAULT_NAMESPACE = "default";

    private final String namespace;
    private final String schemaName;
    private final String tableName;

    public TableId(String namespace, String schemaName, String tableName) {
        this.namespace = Objects.requireNonNull(namespace);
        this.schemaName = Objects.requireNonNull(schemaName);
        this.tableName = Objects.requireNonNull(tableName);
    }

    public static TableId of(String namespace, String schemaName, String tableName) {
        return new TableId(namespace, schemaName, tableName);
    }

    public static TableId of(String schemaName, String tableName) {
        return new TableId(DEFAULT_NAMESPACE, schemaName, tableName);
    }

    public static TableId parse(String tableId) {
        String[] parts = Objects.requireNonNull(tableId).split("\\.");
        if (parts.length == 3) {
            return of(parts[0], parts[1], parts[2]);
        } else if (parts.length == 2) {
            return of(parts[0], parts[1]);
        }
        throw new IllegalArgumentException("Invalid tableId: " + tableId);
    }

    public String identifier() {
        return namespace + "." + schemaName + "." + tableName;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableId that = (TableId) o;
        return Objects.equals(namespace, that.namespace)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, schemaName, tableName);
    }

    @Override
    public String toString() {
        return identifier();
    }
}
