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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineColumnDescription;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/** Tests for {@link TDengineMetadataApplier}. */
class TDengineMetadataApplierTest {

    @Test
    void testApplyCreateTableCreatesDatabaseAndStable() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        TDengineDataSinkConfig config = TDengineTestUtils.defaultConfig();
        TDengineTestUtils.registerExpectedStableDescription(
                client, "metrics", TDengineTestUtils.SCHEMA, config);
        TDengineMetadataApplier applier = new TDengineMetadataApplier(config, ignored -> client);

        applier.applySchemaChange(
                new CreateTableEvent(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA));

        Assertions.assertThat(client.sqls)
                .containsExactly(
                        "CREATE DATABASE IF NOT EXISTS `test_db`",
                        "CREATE STABLE IF NOT EXISTS `test_db`.`metrics` "
                                + "(`ts` TIMESTAMP, `temperature` DOUBLE, `status` NCHAR(256)) "
                                + "TAGS (`location` NCHAR(256))");
        Assertions.assertThat(client.closeCount).isEqualTo(1);
    }

    @Test
    void testApplyAddColumnAddsOnlyMetricColumn() throws Exception {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        TDengineMetadataApplier applier =
                new TDengineMetadataApplier(TDengineTestUtils.defaultConfig(), ignored -> client);

        applier.applySchemaChange(
                new AddColumnEvent(
                        TDengineTestUtils.TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("humidity", DataTypes.DOUBLE())))));

        Assertions.assertThat(client.sqls)
                .containsExactly("ALTER STABLE `test_db`.`metrics` ADD COLUMN `humidity` DOUBLE");
    }

    @Test
    void testRejectMismatchedStableSchema() {
        TDengineTestUtils.RecordingClient client = new TDengineTestUtils.RecordingClient();
        client.stableDescriptions.put(
                "metrics",
                Arrays.asList(
                        new TDengineColumnDescription("ts", "TIMESTAMP", false),
                        new TDengineColumnDescription("temperature", "FLOAT", false),
                        new TDengineColumnDescription("location", "NCHAR(256)", true)));
        TDengineMetadataApplier applier =
                new TDengineMetadataApplier(TDengineTestUtils.defaultConfig(), ignored -> client);

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(
                                                TDengineTestUtils.TABLE_ID,
                                                TDengineTestUtils.SCHEMA)))
                .hasMessageContaining("schema mismatch");
    }

    @Test
    void testRejectAddSubtableColumn() {
        TDengineMetadataApplier applier =
                new TDengineMetadataApplier(
                        TDengineTestUtils.defaultConfig(),
                        ignored -> new TDengineTestUtils.RecordingClient());

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new AddColumnEvent(
                                                TDengineTestUtils.TABLE_ID,
                                                Collections.singletonList(
                                                        AddColumnEvent.last(
                                                                Column.physicalColumn(
                                                                        "device_id",
                                                                        DataTypes.STRING()))))))
                .hasMessageContaining("subtable");
    }
}
