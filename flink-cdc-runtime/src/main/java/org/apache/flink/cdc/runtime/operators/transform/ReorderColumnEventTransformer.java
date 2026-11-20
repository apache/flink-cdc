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

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.ReorderColumnEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;

import javax.annotation.CheckReturnValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Transforms source reorder events into moves that apply to post-transformed schemas. */
@Internal
public final class ReorderColumnEventTransformer {

    private ReorderColumnEventTransformer() {}

    @CheckReturnValue
    public static Optional<ReorderColumnEvent> transformReorderColumnEvent(
            ReorderColumnEvent event, Schema schemaBeforeChange, Schema schemaAfterChange) {
        List<String> oldColumnNames = schemaBeforeChange.getColumnNames();
        List<String> newColumnNames = schemaAfterChange.getColumnNames();
        if (oldColumnNames.equals(newColumnNames)) {
            return Optional.empty();
        }

        Preconditions.checkArgument(
                oldColumnNames.size() == newColumnNames.size()
                        && new HashSet<>(oldColumnNames).equals(new HashSet<>(newColumnNames)),
                "Unable to transform ReorderColumnEvent %s between schemas with different columns: %s and %s",
                event,
                oldColumnNames,
                newColumnNames);

        List<String> stableAnchors = longestCommonSubsequence(oldColumnNames, newColumnNames);
        Map<String, Integer> newColumnPositions = new HashMap<>();
        for (int i = 0; i < newColumnNames.size(); i++) {
            newColumnPositions.put(newColumnNames.get(i), i);
        }
        List<ReorderColumnEvent.ColumnMove> columnMoves = new ArrayList<>();
        if (stableAnchors.isEmpty()) {
            for (int i = newColumnNames.size() - 1; i >= 0; i--) {
                columnMoves.add(ReorderColumnEvent.ColumnMove.first(newColumnNames.get(i)));
            }
        } else {
            int targetIndex = 0;
            for (int anchorIndex = 0; anchorIndex < stableAnchors.size(); anchorIndex++) {
                String anchor = stableAnchors.get(anchorIndex);
                int anchorTargetIndex = newColumnPositions.get(anchor);
                if (anchorIndex == 0) {
                    for (int i = anchorTargetIndex - 1; i >= targetIndex; i--) {
                        columnMoves.add(ReorderColumnEvent.ColumnMove.first(newColumnNames.get(i)));
                    }
                } else {
                    for (int i = targetIndex; i < anchorTargetIndex; i++) {
                        columnMoves.add(
                                ReorderColumnEvent.ColumnMove.before(
                                        newColumnNames.get(i), anchor));
                    }
                }
                targetIndex = anchorTargetIndex + 1;
            }
            for (int i = targetIndex; i < newColumnNames.size(); i++) {
                columnMoves.add(ReorderColumnEvent.ColumnMove.last(newColumnNames.get(i)));
            }
        }

        return Optional.of(new ReorderColumnEvent(event.tableId(), columnMoves));
    }

    private static List<String> longestCommonSubsequence(
            List<String> oldColumnNames, List<String> newColumnNames) {
        int[][] lengths = new int[oldColumnNames.size() + 1][newColumnNames.size() + 1];
        for (int oldIndex = oldColumnNames.size() - 1; oldIndex >= 0; oldIndex--) {
            for (int newIndex = newColumnNames.size() - 1; newIndex >= 0; newIndex--) {
                if (oldColumnNames.get(oldIndex).equals(newColumnNames.get(newIndex))) {
                    lengths[oldIndex][newIndex] = lengths[oldIndex + 1][newIndex + 1] + 1;
                } else {
                    lengths[oldIndex][newIndex] =
                            Math.max(
                                    lengths[oldIndex + 1][newIndex],
                                    lengths[oldIndex][newIndex + 1]);
                }
            }
        }

        List<String> stableAnchors = new ArrayList<>();
        int oldIndex = 0;
        int newIndex = 0;
        while (oldIndex < oldColumnNames.size() && newIndex < newColumnNames.size()) {
            if (oldColumnNames.get(oldIndex).equals(newColumnNames.get(newIndex))) {
                stableAnchors.add(oldColumnNames.get(oldIndex));
                oldIndex++;
                newIndex++;
            } else if (lengths[oldIndex + 1][newIndex] >= lengths[oldIndex][newIndex + 1]) {
                oldIndex++;
            } else {
                newIndex++;
            }
        }
        return stableAnchors;
    }
}
