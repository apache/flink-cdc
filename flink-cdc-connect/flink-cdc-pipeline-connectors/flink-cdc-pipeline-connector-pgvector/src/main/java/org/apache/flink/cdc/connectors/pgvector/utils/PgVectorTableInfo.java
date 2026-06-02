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

package org.apache.flink.cdc.connectors.pgvector.utils;

import java.io.Serializable;
import java.util.Objects;

/** Resolved PostgreSQL target table name. */
public class PgVectorTableInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String schemaName;
    private final String tableName;

    public PgVectorTableInfo(String schemaName, String tableName) {
        this.schemaName = Objects.requireNonNull(schemaName);
        this.tableName = Objects.requireNonNull(tableName);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String identifier() {
        return schemaName + "." + tableName;
    }
}
