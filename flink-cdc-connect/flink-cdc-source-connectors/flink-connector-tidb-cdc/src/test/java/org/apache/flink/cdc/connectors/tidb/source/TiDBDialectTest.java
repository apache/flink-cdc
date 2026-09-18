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

package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Test for {@link TiDBDialect}. */
public class TiDBDialectTest extends TiDBTestBase {
    private static final String databaseName = "customer";
    private static final String tableName = "customers";

    @Test
    public void testDiscoverDataCollectionsInMultiDatabases() {
        initializeTidbTable("customer");
        TiDBSourceConfigFactory configFactoryOfCustomDatabase =
                getMockTiDBSourceConfigFactory(databaseName, null, tableName, 10);

        TiDBDialect dialectOfcustomDatabase =
                new TiDBDialect(configFactoryOfCustomDatabase.create(0));
        List<TableId> tableIdsOfcustomDatabase =
                dialectOfcustomDatabase.discoverDataCollections(
                        configFactoryOfCustomDatabase.create(0));
        Assertions.assertThat(tableIdsOfcustomDatabase.get(0).toString())
                .isEqualTo("customer.customers");
    }
}
