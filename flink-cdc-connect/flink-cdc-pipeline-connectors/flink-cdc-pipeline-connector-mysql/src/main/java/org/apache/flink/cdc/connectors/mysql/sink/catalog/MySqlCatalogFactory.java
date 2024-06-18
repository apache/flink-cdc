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

package org.apache.flink.cdc.connectors.mysql.sink.catalog;

import org.apache.flink.cdc.connectors.jdbc.catalog.CatalogFactory;
import org.apache.flink.cdc.connectors.mysql.factory.MySqlDataSinkFactory;
import org.apache.flink.cdc.connectors.mysql.sink.MySqlDataSinkConfig;
import org.apache.flink.cdc.connectors.mysql.sink.MysqlPooledDataSinkFactory;

/** Factory for creating catalog of mysql. */
public class MySqlCatalogFactory implements CatalogFactory<MySqlDataSinkConfig> {
    public static final MySqlCatalogFactory INSTANCE = new MySqlCatalogFactory();

    @Override
    public MySqlCatalog createCatalog(MySqlDataSinkConfig option) {
        return new MySqlCatalog(
                MySqlDataSinkFactory.IDENTIFIER, option, MysqlPooledDataSinkFactory.INSTANCE);
    }
}
