/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.schema;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

/** Provides as a tool class to obtain table schema information. */
public interface BaseSchema {

    /**
     * Gets table schema for the given table path. e.g. request to MySQL server by running `SHOW
     * CREATE TABLE` if cache missed.
     *
     * @param jdbc jdbc connection.
     * @param tableId Unique identifier for a database table.
     * @return An abstract representation of the structure to the tables of a relational database.
     */
    TableChange getTableSchema(JdbcConnection jdbc, TableId tableId);
}
