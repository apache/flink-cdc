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

package org.apache.flink.cdc.common.event.visitor;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.utils.Preconditions;

import javax.annotation.Nonnull;

/** Visitor clas for all {@link SchemaChangeEvent}s and returns a specific typed object. */
@Internal
public class SchemaChangeEventVisitor {
    public static <T, E extends Throwable> T visit(
            SchemaChangeEvent event,
            @Nonnull VisitorHandler<AddColumnEvent, T, E> addColumnVisitor,
            @Nonnull VisitorHandler<AlterColumnTypeEvent, T, E> alterColumnTypeEventVisitor,
            @Nonnull VisitorHandler<CreateTableEvent, T, E> createTableEventVisitor,
            @Nonnull VisitorHandler<DropColumnEvent, T, E> dropColumnEventVisitor,
            @Nonnull VisitorHandler<DropTableEvent, T, E> dropTableEventVisitor,
            @Nonnull VisitorHandler<RenameColumnEvent, T, E> renameColumnEventVisitor,
            @Nonnull VisitorHandler<TruncateTableEvent, T, E> truncateTableEventVisitor,
            @Nonnull VisitorHandler<AlterTableCommentEvent, T, E> alterTableCommentEventVisitor)
            throws E {
        if (event instanceof AddColumnEvent) {
            Preconditions.checkNotNull(addColumnVisitor);
            return addColumnVisitor.visit((AddColumnEvent) event);
        } else if (event instanceof AlterColumnTypeEvent) {
            Preconditions.checkNotNull(alterColumnTypeEventVisitor);
            return alterColumnTypeEventVisitor.visit((AlterColumnTypeEvent) event);
        } else if (event instanceof CreateTableEvent) {
            Preconditions.checkNotNull(createTableEventVisitor);
            return createTableEventVisitor.visit((CreateTableEvent) event);
        } else if (event instanceof DropColumnEvent) {
            Preconditions.checkNotNull(dropColumnEventVisitor);
            return dropColumnEventVisitor.visit((DropColumnEvent) event);
        } else if (event instanceof DropTableEvent) {
            Preconditions.checkNotNull(dropTableEventVisitor);
            return dropTableEventVisitor.visit((DropTableEvent) event);
        } else if (event instanceof RenameColumnEvent) {
            Preconditions.checkNotNull(renameColumnEventVisitor);
            return renameColumnEventVisitor.visit((RenameColumnEvent) event);
        } else if (event instanceof TruncateTableEvent) {
            Preconditions.checkNotNull(truncateTableEventVisitor);
            return truncateTableEventVisitor.visit((TruncateTableEvent) event);
        } else if (event instanceof AlterTableCommentEvent) {
            Preconditions.checkNotNull(alterTableCommentEventVisitor);
            return alterTableCommentEventVisitor.visit((AlterTableCommentEvent) event);
        } else {
            throw new IllegalArgumentException(
                    "Unknown schema change event type " + event.getType());
        }
    }

    public static <E extends Throwable> void voidVisit(
            SchemaChangeEvent event,
            @Nonnull VoidVisitorHandler<AddColumnEvent, E> addColumnVisitor,
            @Nonnull VoidVisitorHandler<AlterColumnTypeEvent, E> alterColumnTypeEventVisitor,
            @Nonnull VoidVisitorHandler<CreateTableEvent, E> createTableEventVisitor,
            @Nonnull VoidVisitorHandler<DropColumnEvent, E> dropColumnEventVisitor,
            @Nonnull VoidVisitorHandler<DropTableEvent, E> dropTableEventVisitor,
            @Nonnull VoidVisitorHandler<RenameColumnEvent, E> renameColumnEventVisitor,
            @Nonnull VoidVisitorHandler<TruncateTableEvent, E> truncateTableEventVisitor,
            @Nonnull VoidVisitorHandler<AlterTableCommentEvent, E> alterTableCommentEventHandler)
            throws E {
        visit(
                event,
                wrapVoidVisitor(addColumnVisitor),
                wrapVoidVisitor(alterColumnTypeEventVisitor),
                wrapVoidVisitor(createTableEventVisitor),
                wrapVoidVisitor(dropColumnEventVisitor),
                wrapVoidVisitor(dropTableEventVisitor),
                wrapVoidVisitor(renameColumnEventVisitor),
                wrapVoidVisitor(truncateTableEventVisitor),
                wrapVoidVisitor(alterTableCommentEventHandler));
    }

    private static <EVT extends SchemaChangeEvent, E extends Throwable>
            VisitorHandler<EVT, Void, E> wrapVoidVisitor(VoidVisitorHandler<EVT, E> handler) {
        Preconditions.checkNotNull(handler);
        return event -> {
            handler.visit(event);
            return null;
        };
    }
}
