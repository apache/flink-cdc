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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.ColumnPosition;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;

import org.apache.fluss.config.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests unsupported schema changes for {@link FlussMetaDataApplier}. */
class FlussMetadataApplierReorderTest {

    @Test
    void testRejectReorderColumnEvent() {
        FlussMetaDataApplier metadataApplier =
                new FlussMetaDataApplier(
                        new Configuration(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        ReorderColumnEvent reorderEvent =
                new ReorderColumnEvent(
                        TableId.tableId("ns", "schema", "table"),
                        Collections.singletonList(
                                new ReorderColumnEvent.ColumnMove(
                                        "name", ColumnPosition.FIRST, null)));

        assertThat(metadataApplier.getSupportedSchemaEvolutionTypes())
                .doesNotContain(SchemaChangeEventType.REORDER_COLUMN);
        assertThatThrownBy(() -> metadataApplier.applySchemaChange(reorderEvent))
                .isExactlyInstanceOf(UnsupportedSchemaChangeEventException.class)
                .extracting(error -> ((SchemaEvolveException) error).getApplyingEvent())
                .isEqualTo(reorderEvent);
    }
}
