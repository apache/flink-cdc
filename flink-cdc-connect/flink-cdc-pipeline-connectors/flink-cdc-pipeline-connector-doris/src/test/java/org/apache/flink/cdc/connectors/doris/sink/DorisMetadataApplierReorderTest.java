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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;

import org.apache.doris.flink.cfg.DorisOptions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DorisMetadataApplierReorderTest {
    @Test
    void testRejectReorderColumnEvent() {
        DorisMetadataApplier applier =
                new DorisMetadataApplier(
                        DorisOptions.builder().setFenodes("localhost:8030").build(),
                        new Configuration());
        ReorderColumnEvent event =
                new ReorderColumnEvent(
                        TableId.tableId("schema", "table"),
                        Collections.singletonList(ReorderColumnEvent.ColumnMove.first("name")));
        assertThat(applier.getSupportedSchemaEvolutionTypes())
                .doesNotContain(SchemaChangeEventType.REORDER_COLUMN);
        assertThatThrownBy(() -> applier.applySchemaChange(event))
                .isExactlyInstanceOf(UnsupportedSchemaChangeEventException.class);
    }
}
