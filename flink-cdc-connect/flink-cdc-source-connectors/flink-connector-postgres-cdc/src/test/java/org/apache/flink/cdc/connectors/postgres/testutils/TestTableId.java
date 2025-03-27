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

package org.apache.flink.cdc.connectors.postgres.testutils;

import static org.apache.flink.cdc.connectors.postgres.source.utils.PostgresQueryUtils.quote;

/** Represents a qualified table name. */
public class TestTableId {
    private final String schemaName;
    private final String tableName;

    public TestTableId(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /** Returns the qualified name to be used in connector configuration. */
    public String toString() {
        return String.format("%s.%s", schemaName, tableName);
    }

    /** Returns the qualified name to be used in SQL. */
    public String toSql() {
        return String.format("%s.%s", quote(schemaName), quote(tableName));
    }
}
