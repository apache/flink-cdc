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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusCollectionUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link MilvusMetadataApplier}. */
class MilvusMetadataApplierTest {

    private static final TableId TABLE_ID = TableId.parse("inventory.docs");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.BIGINT().notNull())
                    .physicalColumn("title", DataTypes.STRING())
                    .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()).notNull())
                    .primaryKey("id")
                    .build();

    @Test
    void testRejectDropColumnSchemaEvolution() {
        MilvusMetadataApplier applier = new MilvusMetadataApplier(MilvusTestUtils.defaultConfig());

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new DropColumnEvent(
                                                TABLE_ID, Collections.singletonList("title"))))
                .isInstanceOf(SchemaEvolveException.class)
                .extracting(exception -> ((SchemaEvolveException) exception).getExceptionMessage())
                .asString()
                .contains("does not support schema evolution type");
    }

    @Test
    void testRejectCollectionCollisionBeforeMilvusAccess() {
        MilvusDataSinkConfig config = MilvusTestUtils.defaultConfig();
        MilvusMetadataApplier applier = new MilvusMetadataApplier(config);
        Map<String, TableId> owners = new HashMap<>();
        MilvusCollectionUtils.registerCollectionName(TableId.parse("db.table-a"), config, owners);

        Assertions.assertThatThrownBy(
                        () ->
                                MilvusCollectionUtils.registerCollectionName(
                                        TableId.parse("db_table_a"), config, owners))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collection name collision");
    }

    @Test
    void testCreateTableCollisionSurfacesAsSchemaEvolveException() {
        MilvusMetadataApplier applier = new MilvusMetadataApplier(MilvusTestUtils.defaultConfig());
        registerCollectionNameForTest(applier, TableId.parse("db.table-a"));

        Assertions.assertThatThrownBy(
                        () ->
                                applier.applySchemaChange(
                                        new CreateTableEvent(TableId.parse("db_table_a"), SCHEMA)))
                .isInstanceOf(SchemaEvolveException.class)
                .extracting(exception -> ((SchemaEvolveException) exception).getExceptionMessage())
                .asString()
                .contains("collection name collision");
    }

    private static void registerCollectionNameForTest(
            MilvusMetadataApplier applier, TableId tableId) {
        applier.registerCollectionNameForTest(tableId);
    }
}
