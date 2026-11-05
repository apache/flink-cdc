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

package org.apache.flink.cdc.connectors.mysql.source.config;

/** The compare mode for string chunk key during incremental snapshot. */
public enum ChunkKeyCompareMode {
    /** Use the default behavior, which relies on Java's natural String comparison. */
    DEFAULT("default"),

    /**
     * Use case-insensitive comparison in Java ({@link String#compareToIgnoreCase}) to align with
     * MySQL's case-insensitive collations (e.g., utf8mb4_general_ci). No SQL changes are needed.
     */
    CASE_INSENSITIVE("case-insensitive"),

    /**
     * Force binary (byte-level) comparison in both MySQL SQL queries and Java ({@link
     * String#compareTo}). This makes the comparison semantics consistent regardless of the table's
     * collation, but may impact index usage during chunk splitting.
     */
    BINARY("binary");

    private final String value;

    ChunkKeyCompareMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ChunkKeyCompareMode fromValue(String value) {
        for (ChunkKeyCompareMode mode : values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        return DEFAULT;
    }
}
