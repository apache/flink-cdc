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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.utils.Preconditions;

import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that represents an {@code ALTER TABLE COMMENT} or {@code ALTER TABLE
 * SET COMMENT} DDL.
 */
@PublicEvolving
public class AlterTableCommentEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;
    private final String comment;

    public AlterTableCommentEvent(TableId tableId, String comment) {
        Preconditions.checkArgument(comment != null, "comment should not be empty.");
        this.tableId = tableId;
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.ALTER_TABLE_COMMENT;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new AlterTableCommentEvent(newTableId, comment);
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AlterTableCommentEvent)) {
            return false;
        }
        AlterTableCommentEvent that = (AlterTableCommentEvent) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, comment);
    }

    @Override
    public String toString() {
        return "AlterTableCommentEvent{"
                + "tableId="
                + tableId
                + ", comment='"
                + comment
                + '\''
                + '}';
    }
}
