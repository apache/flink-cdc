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

package org.apache.flink.cdc.connectors.mariadb.table;

import org.apache.flink.cdc.connectors.mysql.table.MySqlTableSourceFactory;

/**
 * Factory for creating table sources that capture change data from MariaDB.
 *
 * <p>This factory extends {@link MySqlTableSourceFactory} to reuse the entire MySQL CDC connector
 * read pipeline. MariaDB and MySQL share the same wire protocol and binary-log-based CDC mechanism,
 * so snapshot and streaming are reused as-is.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li>Identified by factory identifier "mariadb-cdc"
 *   <li>Reuses all MySQL CDC connector options (startup mode, "server-id", incremental
 *       snapshot, etc.);
 * </ul>
 *
 * @see org.apache.flink.cdc.connectors.mysql.table.MySqlTableSourceFactory base MySQL
 *     implementation
 */
public class MariaDbTableSourceFactory extends MySqlTableSourceFactory {

    private static final String IDENTIFIER = "mariadb-cdc";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
