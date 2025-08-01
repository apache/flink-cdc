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

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.cdc.connectors.mysql.table.MySqlTableSourceFactory;

/**
 * Factory class for creating table sources that capture data changes from OceanBase databases.
 *
 * <p>This factory extends {@link MySqlTableSourceFactory} to reuse MySQL CDC connector.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li>Identified by factory identifier "oceanbase-cdc"
 *   <li>Compatible with OceanBase Binlog Service
 * </ul>
 *
 * @see org.apache.flink.cdc.connectors.mysql.table.MySqlTableSourceFactory Base MySQL
 *     implementation
 */
public class OceanBaseMySqlTableSourceFactory extends MySqlTableSourceFactory {

    private static final String IDENTIFIER = "oceanbase-cdc";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
