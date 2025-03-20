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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TruncateTableEvent;

/** Visitor clas for all {@link SchemaChangeEvent}s and returns a specific typed object. */
@Internal
public class SchemaChangeEventVisitor {
    public static <T, E extends Throwable> T visit(
            SchemaChangeEvent event,
            AddColumnEventVisitor<T, E> addColumnVisitor,
            AlterColumnTypeEventVisitor<T, E> alterColumnTypeEventVisitor,
            CreateTableEventVisitor<T, E> createTableEventVisitor,
            DropColumnEventVisitor<T, E> dropColumnEventVisitor,
            DropTableEventVisitor<T, E> dropTableEventVisitor,
            RenameColumnEventVisitor<T, E> renameColumnEventVisitor,
            TruncateTableEventVisitor<T, E> truncateTableEventVisitor)
            throws E {
        if (event instanceof AddColumnEvent) {
            if (addColumnVisitor == null) {
                return null;
            }
            return addColumnVisitor.visit((AddColumnEvent) event);
        } else if (event instanceof AlterColumnTypeEvent) {
            if (alterColumnTypeEventVisitor == null) {
                return null;
            }
            return alterColumnTypeEventVisitor.visit((AlterColumnTypeEvent) event);
        } else if (event instanceof CreateTableEvent) {
            if (createTableEventVisitor == null) {
                return null;
            }
            return createTableEventVisitor.visit((CreateTableEvent) event);
        } else if (event instanceof DropColumnEvent) {
            if (dropColumnEventVisitor == null) {
                return null;
            }
            return dropColumnEventVisitor.visit((DropColumnEvent) event);
        } else if (event instanceof DropTableEvent) {
            if (dropTableEventVisitor == null) {
                return null;
            }
            return dropTableEventVisitor.visit((DropTableEvent) event);
        } else if (event instanceof RenameColumnEvent) {
            if (renameColumnEventVisitor == null) {
                return null;
            }
            return renameColumnEventVisitor.visit((RenameColumnEvent) event);
        } else if (event instanceof TruncateTableEvent) {
            if (truncateTableEventVisitor == null) {
                return null;
            }
            return truncateTableEventVisitor.visit((TruncateTableEvent) event);
        } else {
            throw new IllegalArgumentException(
                    "Unknown schema change event type " + event.getType());
        }
    }
}
