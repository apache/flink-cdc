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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ReorderColumnEventTransformer}. */
class ReorderColumnEventTransformerTest {

    private static final TableId TABLE_ID = TableId.parse("default.default.table1");

    @Test
    void testTransformAgainstSchemasWithComputedColumns() {
        Map<String, String> metadata = Collections.singletonMap("source", "mysql");
        ReorderColumnEvent sourceEvent =
                new ReorderColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(ReorderColumnEvent.ColumnMove.last("id")));
        Schema oldTrailingComputedSchema = schemaWithColumns("id", "name", "computed");
        Schema newTrailingComputedSchema = schemaWithColumns("name", "id", "computed");

        ReorderColumnEvent trailingComputedEvent =
                ReorderColumnEventTransformer.transformReorderColumnEvent(
                                sourceEvent, oldTrailingComputedSchema, newTrailingComputedSchema)
                        .orElseThrow(AssertionError::new);

        assertThat(trailingComputedEvent)
                .isEqualTo(
                        new ReorderColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        ReorderColumnEvent.ColumnMove.before("id", "computed"))));
        Schema onceApplied =
                SchemaUtils.applySchemaChangeEvent(
                        oldTrailingComputedSchema, trailingComputedEvent);
        assertThat(onceApplied).isEqualTo(newTrailingComputedSchema);
        assertThat(SchemaUtils.applySchemaChangeEvent(onceApplied, trailingComputedEvent))
                .isEqualTo(newTrailingComputedSchema);

        Schema oldLeadingComputedSchema = schemaWithColumns("computed", "id", "name");
        Schema newLeadingComputedSchema = schemaWithColumns("computed", "name", "id");

        assertThat(
                        ReorderColumnEventTransformer.transformReorderColumnEvent(
                                sourceEvent, oldLeadingComputedSchema, newLeadingComputedSchema))
                .contains(
                        new ReorderColumnEvent(
                                TABLE_ID,
                                Collections.singletonList(
                                        ReorderColumnEvent.ColumnMove.last("id"))));
        assertThat(
                        ReorderColumnEventTransformer.transformReorderColumnEvent(
                                sourceEvent, newLeadingComputedSchema, newLeadingComputedSchema))
                .isEmpty();
    }

    @Test
    void testTransformEveryFourColumnPermutation() {
        Schema originalSchema = schemaWithColumns("A", "B", "C", "D");
        ReorderColumnEvent sourceEvent =
                new ReorderColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(ReorderColumnEvent.ColumnMove.last("A")));

        for (List<String> targetOrder : permutations(originalSchema.getColumnNames())) {
            Schema targetSchema =
                    originalSchema.copy(
                            targetOrder.stream()
                                    .map(
                                            columnName ->
                                                    originalSchema
                                                            .getColumn(columnName)
                                                            .orElseThrow(AssertionError::new))
                                    .collect(Collectors.toList()));
            Optional<ReorderColumnEvent> transformedEvent =
                    ReorderColumnEventTransformer.transformReorderColumnEvent(
                            sourceEvent, originalSchema, targetSchema);

            if (targetOrder.equals(originalSchema.getColumnNames())) {
                assertThat(transformedEvent).isEmpty();
                continue;
            }

            ReorderColumnEvent reorderColumnEvent =
                    transformedEvent.orElseThrow(AssertionError::new);
            Schema reorderedSchema =
                    SchemaUtils.applySchemaChangeEvent(originalSchema, reorderColumnEvent);
            assertThat(reorderedSchema).as("target order %s", targetOrder).isEqualTo(targetSchema);
            assertThat(SchemaUtils.applySchemaChangeEvent(reorderedSchema, reorderColumnEvent))
                    .as("replaying target order %s", targetOrder)
                    .isEqualTo(targetSchema);

            Set<String> movedColumns =
                    reorderColumnEvent.getColumnMoves().stream()
                            .map(ReorderColumnEvent.ColumnMove::getColumnName)
                            .collect(Collectors.toSet());
            assertThat(reorderColumnEvent.getColumnMoves())
                    .filteredOn(move -> move.getReferenceColumnName() != null)
                    .allMatch(move -> !movedColumns.contains(move.getReferenceColumnName()));
        }
    }

    @Test
    void testRejectsDifferentColumnSets() {
        Schema oldSchema = schemaWithColumns("id", "name");
        Schema newSchema = schemaWithColumns("id", "computed");
        ReorderColumnEvent sourceEvent =
                new ReorderColumnEvent(
                        TABLE_ID,
                        Collections.singletonList(ReorderColumnEvent.ColumnMove.last("id")));

        assertThatThrownBy(
                        () ->
                                ReorderColumnEventTransformer.transformReorderColumnEvent(
                                        sourceEvent, oldSchema, newSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemas with different columns");
    }

    private static Schema schemaWithColumns(String... columnNames) {
        Schema.Builder builder = Schema.newBuilder();
        for (String columnName : columnNames) {
            builder.physicalColumn(columnName, DataTypes.INT());
        }
        return builder.build();
    }

    private static List<List<String>> permutations(List<String> values) {
        List<List<String>> results = new ArrayList<>();
        collectPermutations(new ArrayList<>(), values, results);
        return results;
    }

    private static void collectPermutations(
            List<String> prefix, List<String> remaining, List<List<String>> results) {
        if (remaining.isEmpty()) {
            results.add(prefix);
            return;
        }
        for (int i = 0; i < remaining.size(); i++) {
            List<String> nextPrefix = new ArrayList<>(prefix);
            nextPrefix.add(remaining.get(i));
            List<String> nextRemaining = new ArrayList<>(remaining);
            nextRemaining.remove(i);
            collectPermutations(nextPrefix, nextRemaining, results);
        }
    }
}
