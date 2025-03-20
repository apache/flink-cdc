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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

/** An enumeration of schema change event types for {@link SchemaChangeEvent}. */
@PublicEvolving
public enum SchemaChangeEventType {
    ADD_COLUMN("add.column"),
    ALTER_COLUMN_TYPE("alter.column.type"),
    CREATE_TABLE("create.table"),
    DROP_COLUMN("drop.column"),
    DROP_TABLE("drop.table"),
    RENAME_COLUMN("rename.column"),
    TRUNCATE_TABLE("truncate.table");

    private final String tag;

    SchemaChangeEventType(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public static SchemaChangeEventType ofEvent(SchemaChangeEvent event) {
        if (event instanceof AddColumnEvent) {
            return ADD_COLUMN;
        } else if (event instanceof AlterColumnTypeEvent) {
            return ALTER_COLUMN_TYPE;
        } else if (event instanceof CreateTableEvent) {
            return CREATE_TABLE;
        } else if (event instanceof DropColumnEvent) {
            return DROP_COLUMN;
        } else if (event instanceof DropTableEvent) {
            return DROP_TABLE;
        } else if (event instanceof RenameColumnEvent) {
            return RENAME_COLUMN;
        } else if (event instanceof TruncateTableEvent) {
            return TRUNCATE_TABLE;
        } else {
            throw new RuntimeException("Unknown schema change event type: " + event.getClass());
        }
    }

    public static SchemaChangeEventType ofTag(String tag) {
        switch (tag) {
            case "add.column":
                return ADD_COLUMN;
            case "alter.column.type":
                return ALTER_COLUMN_TYPE;
            case "create.table":
                return CREATE_TABLE;
            case "drop.column":
                return DROP_COLUMN;
            case "drop.table":
                return DROP_TABLE;
            case "rename.column":
                return RENAME_COLUMN;
            case "truncate.table":
                return TRUNCATE_TABLE;
            default:
                throw new RuntimeException("Unknown schema change event type: " + tag);
        }
    }
}
