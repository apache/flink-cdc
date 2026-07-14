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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.TargetTableCreateMode;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TargetTableCreateModeMetadataApplier}. */
class TargetTableCreateModeMetadataApplierTest {

    private static final TableId TABLE_ID = TableId.tableId("db", "table");
    private static final CreateTableEvent CREATE_TABLE_EVENT =
            new CreateTableEvent(
                    TABLE_ID,
                    Schema.newBuilder()
                            .physicalColumn("id", DataTypes.INT())
                            .physicalColumn("name", DataTypes.STRING())
                            .build());

    @Test
    void testCreateIfNotExistsDelegatesCreateTableEvent() {
        TestMetadataApplier delegate = new TestMetadataApplier(true);

        MetadataApplier applier =
                TargetTableCreateModeMetadataApplier.wrap(
                        delegate, TargetTableCreateMode.CREATE_IF_NOT_EXISTS, "test");
        applier.applySchemaChange(CREATE_TABLE_EVENT);

        assertThat(delegate.applySchemaChangeCalled).isTrue();
    }

    @Test
    void testErrorIfNotExistsFailsWhenTargetTableDoesNotExist() {
        TestMetadataApplier delegate = new TestMetadataApplier(false);

        MetadataApplier applier =
                TargetTableCreateModeMetadataApplier.wrap(
                        delegate, TargetTableCreateMode.ERROR_IF_NOT_EXISTS, "test");

        assertThatThrownBy(() -> applier.applySchemaChange(CREATE_TABLE_EVENT))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("Target table does not exist");
        assertThat(delegate.applySchemaChangeCalled).isFalse();
    }

    @Test
    void testErrorIfNotExistsDelegatesCreateTableEventWhenTargetTableExists() {
        TestMetadataApplier delegate = new TestMetadataApplier(true);

        MetadataApplier applier =
                TargetTableCreateModeMetadataApplier.wrap(
                        delegate, TargetTableCreateMode.ERROR_IF_NOT_EXISTS, "test");
        applier.applySchemaChange(CREATE_TABLE_EVENT);

        assertThat(delegate.applySchemaChangeCalled).isTrue();
    }

    @Test
    void testErrorIfNotExistsDelegatesNonCreateTableEvent() {
        TestMetadataApplier delegate = new TestMetadataApplier(true);

        MetadataApplier applier =
                TargetTableCreateModeMetadataApplier.wrap(
                        delegate, TargetTableCreateMode.ERROR_IF_NOT_EXISTS, "test");
        applier.applySchemaChange(new DropTableEvent(TABLE_ID));

        assertThat(delegate.applySchemaChangeCalled).isTrue();
    }

    @Test
    void testErrorIfNotExistsRequiresExistenceCheckCapability() {
        assertThatThrownBy(
                        () ->
                                TargetTableCreateModeMetadataApplier.wrap(
                                        new MetadataApplier() {
                                            @Override
                                            public void applySchemaChange(
                                                    SchemaChangeEvent schemaChangeEvent) {}
                                        },
                                        TargetTableCreateMode.ERROR_IF_NOT_EXISTS,
                                        "test"))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("target.table.create.mode=ERROR_IF_NOT_EXISTS");
    }

    private static class TestMetadataApplier
            implements MetadataApplier, SupportsTargetTableExistenceCheck {

        private final boolean targetTableExists;
        private boolean applySchemaChangeCalled;

        private TestMetadataApplier(boolean targetTableExists) {
            this.targetTableExists = targetTableExists;
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            applySchemaChangeCalled = true;
        }

        @Override
        public boolean targetTableExists(TableId tableId) {
            return targetTableExists;
        }
    }
}
