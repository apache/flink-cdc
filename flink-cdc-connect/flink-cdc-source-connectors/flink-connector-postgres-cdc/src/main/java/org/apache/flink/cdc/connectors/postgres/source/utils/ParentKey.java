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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import org.apache.flink.cdc.common.utils.Predicates;

/**
 * Helper class for parent table matching.
 *
 * <p>This class represents a parent table identifier with optional schema, used for matching and
 * excluding parent tables in partition-aware filtering.
 */
class ParentKey {
    private final String schema;
    private final String table;

    ParentKey(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    /**
     * Creates a ParentKey from an array of string parts.
     *
     * <p>Supports various formats:
     *
     * <ul>
     *   <li>Single part: table only (schema = null)
     *   <li>Two parts: schema.table
     *   <li>Three parts: catalog.schema.table (uses schema.table)
     * </ul>
     *
     * @param parts the array of string parts
     * @return a ParentKey instance, or null if parts is null or empty
     */
    static ParentKey from(String[] parts) {
        if (parts == null || parts.length == 0) {
            return null;
        }
        if (parts.length == 1) {
            return new ParentKey(null, parts[0]);
        }
        if (parts.length == 2) {
            return new ParentKey(parts[0], parts[1]);
        }
        if (parts.length == 3) {
            return new ParentKey(parts[1], parts[2]);
        }
        return null;
    }

    /**
     * Creates a ParentKey from a table pattern string.
     *
     * @param pattern the table pattern string (e.g., "public.orders" or "orders")
     * @return a ParentKey instance
     */
    static ParentKey fromPattern(String pattern) {
        if (pattern == null || pattern.trim().isEmpty()) {
            return null;
        }
        String[] parts = Predicates.RegExSplitterByDot.split(pattern.trim());
        return from(parts);
    }

    /**
     * Checks if this ParentKey matches another ParentKey.
     *
     * <p>Matching rules:
     *
     * <ul>
     *   <li>If this schema is null or empty, matches any schema (table-only match)
     *   <li>Otherwise, requires both schema and table to match exactly
     * </ul>
     *
     * @param other the other ParentKey to match against
     * @return true if the keys match, false otherwise
     */
    boolean matches(ParentKey other) {
        if (other == null) {
            return false;
        }
        if (schema == null || schema.isEmpty()) {
            return table.equals(other.table);
        }
        return schema.equals(other.schema) && table.equals(other.table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParentKey parentKey = (ParentKey) o;
        return table.equals(parentKey.table)
                && (schema == null ? parentKey.schema == null : schema.equals(parentKey.schema));
    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + table.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ParentKey{" + "schema='" + schema + '\'' + ", table='" + table + '\'' + '}';
    }
}
