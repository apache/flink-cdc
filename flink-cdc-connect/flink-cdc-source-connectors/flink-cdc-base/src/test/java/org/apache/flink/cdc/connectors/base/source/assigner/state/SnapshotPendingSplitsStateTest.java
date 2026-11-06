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

package org.apache.flink.cdc.connectors.base.source.assigner.state;

import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.meta.split.SchemalessSnapshotSplit;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link SnapshotPendingSplitsState}. */
class SnapshotPendingSplitsStateTest {

    /**
     * Connectors compiled against earlier versions of flink-cdc-base link against these exact
     * method descriptors, so the parameter and return types must stay {@link Map}.
     */
    @Test
    void testPublicSignaturesKeepMapType() throws Exception {
        assertThat(SnapshotPendingSplitsState.class.getMethod("getAssignedSplits").getReturnType())
                .isEqualTo(Map.class);
        assertThatCode(
                        () ->
                                SnapshotPendingSplitsState.class.getConstructor(
                                        List.class,
                                        List.class,
                                        Map.class,
                                        Map.class,
                                        Map.class,
                                        AssignerStatus.class,
                                        List.class,
                                        boolean.class,
                                        boolean.class,
                                        Map.class,
                                        ChunkSplitterState.class))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                SnapshotPendingSplitsState.class.getConstructor(
                                        List.class,
                                        List.class,
                                        Map.class,
                                        Map.class,
                                        Map.class,
                                        AssignerStatus.class,
                                        List.class,
                                        boolean.class,
                                        boolean.class,
                                        Map.class,
                                        ChunkSplitterState.class,
                                        boolean.class))
                .doesNotThrowAnyException();
    }

    @Test
    void testAssignedSplitsKeepIterationOrderOfGivenMap() {
        TableId tableId = TableId.parse("catalog.schema.table");
        RowType splitKeyType =
                new RowType(
                        Collections.singletonList(new RowType.RowField("id", new BigIntType())));
        // not in lexicographic order of split ids
        List<String> assignmentOrder =
                Arrays.asList(tableId + ":2", tableId + ":10", tableId + ":1");
        Map<String, SchemalessSnapshotSplit> assignedSplits = new LinkedHashMap<>();
        for (String splitId : assignmentOrder) {
            assignedSplits.put(
                    splitId,
                    new SchemalessSnapshotSplit(tableId, splitId, splitKeyType, null, null, null));
        }

        SnapshotPendingSplitsState state =
                new SnapshotPendingSplitsState(
                        Collections.singletonList(tableId),
                        Collections.emptyList(),
                        assignedSplits,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        AssignerStatus.INITIAL_ASSIGNING_FINISHED,
                        Collections.emptyList(),
                        false,
                        true,
                        Collections.emptyMap(),
                        ChunkSplitterState.NO_SPLITTING_TABLE_STATE);

        assertThat(new ArrayList<>(state.getAssignedSplits().keySet())).isEqualTo(assignmentOrder);
    }
}
