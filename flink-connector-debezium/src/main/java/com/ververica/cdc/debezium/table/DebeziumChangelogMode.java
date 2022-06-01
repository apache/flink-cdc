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

package com.ververica.cdc.debezium.table;

/** Changelog modes used to encode changes from Debezium to Flink internal structure. */
public enum DebeziumChangelogMode {
    /** Encodes changes as retract stream using all RowKinds. This is the default mode. */
    ALL("all"),
    /**
     * Encodes changes as upsert stream that describes idempotent updates on a key. Primary keys
     * must be set in tables to use this changelog mode.
     */
    UPSERT("upsert");

    private final String value;

    DebeziumChangelogMode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
