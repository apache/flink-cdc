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

package org.apache.flink.cdc.connectors.hudi.sink.coordinator;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.configuration.Configuration;

import org.apache.hudi.configuration.FlinkOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MultiTableStreamWriteOperatorCoordinator}. */
class MultiTableStreamWriteOperatorCoordinatorTest {

    @Test
    void testApplyHiveSyncTableDefaultsDerivesFromTableIdWhenUnset() {
        Configuration tableConfig = new Configuration();
        TableId tableId = TableId.parse("my_db.my_table");

        MultiTableStreamWriteOperatorCoordinator.applyHiveSyncTableDefaults(tableConfig, tableId);

        assertThat(tableConfig.get(FlinkOptions.HIVE_SYNC_DB)).isEqualTo(tableId.getSchemaName());
        assertThat(tableConfig.get(FlinkOptions.HIVE_SYNC_TABLE)).isEqualTo(tableId.getTableName());
    }

    @Test
    void testApplyHiveSyncTableDefaultsPreservesExplicitUserValues() {
        Configuration tableConfig = new Configuration();
        tableConfig.setString(FlinkOptions.HIVE_SYNC_DB.key(), "custom_db");
        tableConfig.setString(FlinkOptions.HIVE_SYNC_TABLE.key(), "custom_table");
        TableId tableId = TableId.parse("my_db.my_table");

        MultiTableStreamWriteOperatorCoordinator.applyHiveSyncTableDefaults(tableConfig, tableId);

        assertThat(tableConfig.get(FlinkOptions.HIVE_SYNC_DB)).isEqualTo("custom_db");
        assertThat(tableConfig.get(FlinkOptions.HIVE_SYNC_TABLE)).isEqualTo("custom_table");
    }

    @Test
    void testApplyHiveSyncTableDefaultsHandlesPartiallySetConfig() {
        Configuration tableConfig = new Configuration();
        tableConfig.setString(FlinkOptions.HIVE_SYNC_DB.key(), "custom_db");
        TableId tableId = TableId.parse("my_db.my_table");

        MultiTableStreamWriteOperatorCoordinator.applyHiveSyncTableDefaults(tableConfig, tableId);

        assertThat(tableConfig.get(FlinkOptions.HIVE_SYNC_DB)).isEqualTo("custom_db");
        assertThat(tableConfig.get(FlinkOptions.HIVE_SYNC_TABLE)).isEqualTo(tableId.getTableName());
    }
}
