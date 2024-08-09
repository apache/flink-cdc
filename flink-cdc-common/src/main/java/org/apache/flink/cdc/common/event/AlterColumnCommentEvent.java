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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.schema.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ALTER COLUMN} DDL, which may contain the
 * comment changes.
 */
public class AlterColumnCommentEvent
        implements ColumnSchemaChangeEvent, SchemaChangeEventWithPreSchema {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    /** key => column name, value => column type after changing. */
    private final Map<String, String> commentMapping;

    private final Map<String, String> oldCommentMapping;

    public AlterColumnCommentEvent(TableId tableId, Map<String, String> commentMapping) {
        this.tableId = tableId;
        this.commentMapping = commentMapping;
        this.oldCommentMapping = new HashMap<>();
    }

    public AlterColumnCommentEvent(
            TableId tableId,
            Map<String, String> commentMapping,
            Map<String, String> oldCommentMapping) {
        this.tableId = tableId;
        this.commentMapping = commentMapping;
        this.oldCommentMapping = oldCommentMapping;
    }

    /** Returns the type mapping. */
    public Map<String, String> getCommentMapping() {
        return commentMapping;
    }

    public Map<String, String> getOldCommentMapping() {
        return oldCommentMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterColumnCommentEvent)) {
            return false;
        }
        AlterColumnCommentEvent that = (AlterColumnCommentEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(commentMapping, that.commentMapping)
                && Objects.equals(oldCommentMapping, that.oldCommentMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, commentMapping, oldCommentMapping);
    }

    @Override
    public String toString() {
        if (hasPreSchema()) {
            return "AlterColumnCommentEvent{"
                    + "tableId="
                    + tableId
                    + ", commentMapping="
                    + commentMapping
                    + ", oldCommentMapping="
                    + oldCommentMapping
                    + '}';
        } else {
            return "AlterColumnCommentEvent{"
                    + "tableId="
                    + tableId
                    + ", commentMapping="
                    + commentMapping
                    + '}';
        }
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public boolean hasPreSchema() {
        return !oldCommentMapping.isEmpty();
    }

    @Override
    public void fillPreSchema(Schema oldTypeSchema) {
        oldCommentMapping.clear();
        oldTypeSchema.getColumns().stream()
                .filter(e -> commentMapping.containsKey(e.getName()))
                .forEach(e -> oldCommentMapping.put(e.getName(), e.getComment()));
    }

    @Override
    public boolean trimRedundantChanges() {
        if (hasPreSchema()) {
            Set<String> redundantlyChangedColumns =
                    commentMapping.keySet().stream()
                            .filter(
                                    e ->
                                            Objects.equals(
                                                    commentMapping.get(e),
                                                    oldCommentMapping.get(e)))
                            .collect(Collectors.toSet());

            // Remove redundant alter column type records that doesn't really change the type
            commentMapping.keySet().removeAll(redundantlyChangedColumns);
            oldCommentMapping.keySet().removeAll(redundantlyChangedColumns);
        }
        return !commentMapping.isEmpty();
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.ALTER_COLUMN_COMMENT;
    }
}
