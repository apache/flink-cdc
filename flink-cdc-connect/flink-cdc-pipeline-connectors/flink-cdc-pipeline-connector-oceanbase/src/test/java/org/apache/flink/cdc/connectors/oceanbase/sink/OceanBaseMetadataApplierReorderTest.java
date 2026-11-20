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

package org.apache.flink.cdc.connectors.oceanbase.sink;

import org.apache.flink.cdc.common.event.ColumnPosition;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseCatalog;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseColumn;
import org.apache.flink.cdc.connectors.oceanbase.catalog.OceanBaseTable;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests unsupported schema changes for {@link OceanBaseMetadataApplier}. */
class OceanBaseMetadataApplierReorderTest {

    @Test
    void testRejectReorderColumnEvent() {
        TrackingOceanBaseCatalog catalog = new TrackingOceanBaseCatalog();
        OceanBaseMetadataApplier metadataApplier = new OceanBaseMetadataApplier(catalog);
        ReorderColumnEvent reorderEvent = reorderEvent();

        assertThatThrownBy(() -> metadataApplier.applySchemaChange(reorderEvent))
                .isExactlyInstanceOf(UnsupportedSchemaChangeEventException.class)
                .extracting(error -> ((SchemaEvolveException) error).getApplyingEvent())
                .isEqualTo(reorderEvent);
        assertThat(metadataApplier.getSupportedSchemaEvolutionTypes())
                .doesNotContain(SchemaChangeEventType.REORDER_COLUMN);
        assertThat(catalog.getInvocationCount()).isZero();
    }

    @Test
    void testAcceptedSchemaEvolutionTypes() {
        OceanBaseMetadataApplier metadataApplier =
                new OceanBaseMetadataApplier(new TrackingOceanBaseCatalog());
        for (SchemaChangeEventType type : SchemaChangeEventTypeFamily.ALL) {
            assertThat(metadataApplier.acceptsSchemaEvolutionType(type)).isTrue();
        }

        metadataApplier.setAcceptedSchemaEvolutionTypes(Collections.emptySet());

        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.REORDER_COLUMN))
                .isFalse();
        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.CREATE_TABLE))
                .isFalse();

        EnumSet<SchemaChangeEventType> acceptedTypes =
                EnumSet.of(SchemaChangeEventType.CREATE_TABLE);
        metadataApplier.setAcceptedSchemaEvolutionTypes(acceptedTypes);
        acceptedTypes.add(SchemaChangeEventType.RENAME_COLUMN);

        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.CREATE_TABLE))
                .isTrue();
        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.RENAME_COLUMN))
                .isFalse();
        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.REORDER_COLUMN))
                .isFalse();
    }

    private static ReorderColumnEvent reorderEvent() {
        return new ReorderColumnEvent(
                TableId.tableId("ns", "schema", "table"),
                Collections.singletonList(
                        new ReorderColumnEvent.ColumnMove("name", ColumnPosition.FIRST, null)));
    }

    private static class TrackingOceanBaseCatalog extends OceanBaseCatalog {

        private int invocationCount;

        private TrackingOceanBaseCatalog() {
            super(new OceanBaseConnectorOptions(Collections.emptyMap()));
        }

        private int getInvocationCount() {
            return invocationCount;
        }

        @Override
        public void open() {
            invocationCount++;
        }

        @Override
        public boolean databaseExists(String databaseName) {
            invocationCount++;
            return false;
        }

        @Override
        public void createDatabase(String databaseName, boolean ignoreIfExists) {
            invocationCount++;
        }

        @Override
        public boolean tableExists(String databaseName, String tableName) {
            invocationCount++;
            return false;
        }

        @Override
        public void createTable(OceanBaseTable table, boolean ignoreIfExists) {
            invocationCount++;
        }

        @Override
        public void alterAddColumns(
                String databaseName, String tableName, List<OceanBaseColumn> addColumns) {
            invocationCount++;
        }

        @Override
        public void alterDropColumns(
                String schemaName, String tableName, List<String> dropColumns) {
            invocationCount++;
        }

        @Override
        public void alterTable(String schemaName, String tableName, String comment) {
            invocationCount++;
        }

        @Override
        public void alterColumnType(
                String schemaName,
                String tableName,
                String columnName,
                DataType dataType,
                String comment) {
            invocationCount++;
        }

        @Override
        public void renameColumn(
                String schemaName, String tableName, String oldColumnName, String newColumnName) {
            invocationCount++;
        }

        @Override
        public void dropTable(String schemaName, String tableName) {
            invocationCount++;
        }

        @Override
        public void truncateTable(String schemaName, String tableName) {
            invocationCount++;
        }

        @Override
        public void close() {
            invocationCount++;
        }
    }
}
