/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.TargetTableCreateMode;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.sink.TargetTableCreateModeMetadataApplier;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.table.api.ValidationException;

import com.aliyun.odps.OdpsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MaxComputeMetadataApplier}. */
class MaxComputeMetadataApplierTest extends EmulatorTestBase {

    private static final TableId MISSING_TABLE =
            TableId.tableId("MC_METADATA_APPLIER_MISSING_TABLE");
    private static final TableId WRAPPER_MISSING_TABLE =
            TableId.tableId("MC_METADATA_APPLIER_WRAPPER_MISSING_TABLE");

    @AfterEach
    void deleteTables() throws OdpsException {
        odpsInstance.tables().delete(MISSING_TABLE.getTableName(), true);
        odpsInstance.tables().delete(WRAPPER_MISSING_TABLE.getTableName(), true);
    }

    @Test
    void testTargetTableExistsReturnsFalseWhenTargetTableIsMissing() {
        MaxComputeMetadataApplier applier = new MaxComputeMetadataApplier(testOptions);

        assertThat(applier.targetTableExists(MISSING_TABLE)).isFalse();
    }

    @Test
    void testErrorIfNotExistsModeFailsWhenTargetTableIsMissing() {
        MaxComputeMetadataApplier applier = new MaxComputeMetadataApplier(testOptions);
        MetadataApplier wrappedApplier =
                TargetTableCreateModeMetadataApplier.wrap(
                        applier, TargetTableCreateMode.ERROR_IF_NOT_EXISTS, "maxcompute");

        assertThatThrownBy(
                        () ->
                                wrappedApplier.applySchemaChange(
                                        new CreateTableEvent(
                                                WRAPPER_MISSING_TABLE, createSchema())))
                .isExactlyInstanceOf(ValidationException.class)
                .hasMessageContaining("Target table does not exist");
        assertThat(applier.targetTableExists(WRAPPER_MISSING_TABLE)).isFalse();
    }

    private static Schema createSchema() {
        return Schema.newBuilder()
                .physicalColumn("PK", DataTypes.BIGINT())
                .physicalColumn("ID1", DataTypes.BIGINT())
                .physicalColumn("ID2", DataTypes.BIGINT())
                .primaryKey("PK")
                .build();
    }
}
