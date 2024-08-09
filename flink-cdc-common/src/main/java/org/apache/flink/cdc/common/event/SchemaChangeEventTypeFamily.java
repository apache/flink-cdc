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

/**
 * An enumeration of schema change event families for clustering {@link SchemaChangeEvent}s into
 * categories.
 */
@PublicEvolving
public class SchemaChangeEventTypeFamily {

    public static final SchemaChangeEventType[] ADD = {SchemaChangeEventType.ADD_COLUMN};

    public static final SchemaChangeEventType[] ALTER = {SchemaChangeEventType.ALTER_COLUMN_TYPE};

    public static final SchemaChangeEventType[] CREATE = {SchemaChangeEventType.CREATE_TABLE};

    public static final SchemaChangeEventType[] DROP = {SchemaChangeEventType.DROP_COLUMN};

    public static final SchemaChangeEventType[] RENAME = {SchemaChangeEventType.RENAME_COLUMN};

    public static final SchemaChangeEventType[] TABLE = {SchemaChangeEventType.CREATE_TABLE};

    public static final SchemaChangeEventType[] COLUMN = {
        SchemaChangeEventType.ADD_COLUMN,
        SchemaChangeEventType.ALTER_COLUMN_TYPE,
        SchemaChangeEventType.DROP_COLUMN,
        SchemaChangeEventType.RENAME_COLUMN
    };

    public static final SchemaChangeEventType[] ALL = {
        SchemaChangeEventType.ADD_COLUMN,
        SchemaChangeEventType.CREATE_TABLE,
        SchemaChangeEventType.ALTER_COLUMN_TYPE,
        SchemaChangeEventType.DROP_COLUMN,
        SchemaChangeEventType.RENAME_COLUMN
    };

    public static final SchemaChangeEventType[] NONE = {};

    public static SchemaChangeEventType[] ofTag(String tag) {
        switch (tag) {
            case "add":
                return ADD;
            case "alter":
                return ALTER;
            case "create":
                return CREATE;
            case "drop":
                return DROP;
            case "rename":
                return RENAME;
            case "table":
                return TABLE;
            case "column":
                return COLUMN;
            case "all":
                return ALL;
            default:
                return NONE;
        }
    }
}
