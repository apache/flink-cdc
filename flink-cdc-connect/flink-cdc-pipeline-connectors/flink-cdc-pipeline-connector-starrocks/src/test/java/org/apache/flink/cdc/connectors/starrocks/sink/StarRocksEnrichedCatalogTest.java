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

package org.apache.flink.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.catalog.StarRocksColumn;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the async {@code MODIFY COLUMN} path.
 *
 * <p>StarRocks schema change is asynchronous. Submitting {@code ALTER} with {@code
 * executeUpdateStatement} returns before the job finishes, so a following STRING value such as
 * {@code hello} is stream-loaded into an INT column and dropped or stored as null. {@code
 * executeAlter} waits until the job is {@code FINISHED}.
 */
class StarRocksEnrichedCatalogTest {

    @Test
    void testAlterColumnTypeWaitsUntilJobFinishedBeforeAcceptingStringValue() {
        AsyncIntToVarcharCatalog catalog = new AsyncIntToVarcharCatalog();
        StarRocksColumn target =
                new StarRocksColumn.Builder()
                        .setColumnName("age")
                        .setOrdinalPosition(0)
                        .setDataType("varchar")
                        .setColumnSize(1048576)
                        .setNullable(true)
                        .build();

        Assertions.assertThat(catalog.canWriteStringAge())
                .as("STRING values cannot be loaded while age is still INT")
                .isFalse();

        catalog.alterColumnType("inventory", "customers", target, 30);

        Assertions.assertThat(catalog.usedExecuteAlter).isTrue();
        Assertions.assertThat(catalog.usedExecuteUpdate).isFalse();
        Assertions.assertThat(catalog.canWriteStringAge()).isTrue();
        Assertions.assertThat(catalog.lastTimeoutSecond).isEqualTo(30);
    }

    /**
     * Models StarRocks: {@code age} stays {@code INT} until the alter job finishes. {@code
     * executeUpdateStatement} only submits SQL; {@code executeAlter} waits and then flips the type.
     */
    private static final class AsyncIntToVarcharCatalog extends StarRocksEnrichedCatalog {

        private boolean alterJobFinished;
        private boolean usedExecuteAlter;
        private boolean usedExecuteUpdate;
        private long lastTimeoutSecond = -1L;

        private AsyncIntToVarcharCatalog() {
            super("jdbc:mysql://127.0.0.1:9030", "root", "");
        }

        @Override
        protected void executeAlter(
                String databaseName, String tableName, String alterSql, long timeoutSecond) {
            usedExecuteAlter = true;
            lastTimeoutSecond = timeoutSecond;
            alterJobFinished = true;
        }

        @Override
        protected void executeUpdateStatement(String sql) throws StarRocksCatalogException {
            usedExecuteUpdate = true;
        }

        private boolean canWriteStringAge() {
            return alterJobFinished;
        }
    }
}
