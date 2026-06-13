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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link LanceDbMetadataApplier}. */
class LanceDbMetadataApplierTest {

    @Test
    void testCreateDataset() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        LanceDbMetadataApplier applier =
                new LanceDbMetadataApplier(LanceDbTestUtils.defaultConfig(), client);

        applier.applySchemaChange(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA));

        Assertions.assertThat(client.schemas).containsKey("/tmp/lancedb/inventory_products.lance");
    }

    @Test
    void testRejectsAddColumnWhenSchemaEvolutionDisabled() {
        LanceDbMetadataApplier applier =
                new LanceDbMetadataApplier(
                        LanceDbTestUtils.defaultConfig(), new LanceDbTestUtils.RecordingClient());

        Assertions.assertThat(applier.acceptsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN))
                .isFalse();
    }

    @Test
    void testAddsColumnWhenSchemaEvolutionEnabled() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        LanceDbMetadataApplier applier =
                new LanceDbMetadataApplier(
                        LanceDbTestUtils.config("append-only", 100, 0, Duration.ZERO, true),
                        client);

        applier.applySchemaChange(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA));
        applier.applySchemaChange(
                new AddColumnEvent(
                        LanceDbTestUtils.TABLE_ID,
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn("stock", DataTypes.INT())))));

        Assertions.assertThat(
                        client.schemas
                                .get("/tmp/lancedb/inventory_products.lance")
                                .findField("stock"))
                .isNotNull();
    }

    @Test
    void testRejectsDatasetPathOwnershipConflict() throws Exception {
        LanceDbTestUtils.RecordingClient client = new LanceDbTestUtils.RecordingClient();
        LanceDbMetadataApplier applier =
                new LanceDbMetadataApplier(configWithPathConflict(), client);

        applier.applySchemaChange(
                new CreateTableEvent(LanceDbTestUtils.TABLE_ID, LanceDbTestUtils.SCHEMA));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(
                                                TableId.parse("inventory.products_b"),
                                                LanceDbTestUtils.SCHEMA)))
                .isInstanceOf(org.apache.flink.cdc.common.exceptions.SchemaEvolveException.class)
                .hasMessageContaining("already owned");
    }

    @Test
    void testRejectsReservedCdcMetadataColumnInMetadataApplier() {
        LanceDbMetadataApplier applier =
                new LanceDbMetadataApplier(
                        LanceDbTestUtils.config(
                                "append-with-metadata", 100, 0, Duration.ZERO, false),
                        new LanceDbTestUtils.RecordingClient());

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(
                                                LanceDbTestUtils.TABLE_ID, schemaWithCdcColumn())))
                .isInstanceOf(SchemaEvolveException.class)
                .satisfies(
                        e ->
                                Assertions.assertThat(
                                                ((SchemaEvolveException) e).getExceptionMessage())
                                        .contains("reserved LanceDB CDC metadata column _cdc_op"));
    }

    private Schema schemaWithCdcColumn() {
        return Schema.newBuilder()
                .physicalColumn("_cdc_op", DataTypes.STRING())
                .physicalColumn("payload", DataTypes.STRING())
                .build();
    }

    private LanceDbDataSinkConfig configWithPathConflict() {
        Map<TableId, String> mappings = new HashMap<>();
        mappings.put(LanceDbTestUtils.TABLE_ID, "/tmp/lancedb/products.lance");
        mappings.put(TableId.parse("inventory.products_b"), "/tmp/lancedb/products.lance");
        return new LanceDbDataSinkConfig(
                "/tmp/lancedb",
                mappings,
                true,
                true,
                true,
                false,
                "append-only",
                100,
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                1024,
                128,
                1024 * 1024L,
                true,
                "CREATE",
                ZoneId.of("UTC"),
                Collections.emptyMap());
    }
}
