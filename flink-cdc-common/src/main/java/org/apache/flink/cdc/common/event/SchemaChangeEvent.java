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

import java.io.Serializable;

/**
 * Class {@code SchemaChangeEvent} represents the changes in the table structure of the external
 * system, such as CREATE, DROP, RENAME and so on.
 */
@PublicEvolving
public interface SchemaChangeEvent extends ChangeEvent, Serializable {
    /** Returns its {@link SchemaChangeEventType}. */
    SchemaChangeEventType getType();

    default <E extends Throwable> void visit(SchemaChangeEventVisitorVoid<E> visitor) throws E {
        if (this instanceof AddColumnEvent) {
            visitor.visit((AddColumnEvent) this);
        } else if (this instanceof AlterColumnTypeEvent) {
            visitor.visit((AlterColumnTypeEvent) this);
        } else if (this instanceof CreateTableEvent) {
            visitor.visit((CreateTableEvent) this);
        } else if (this instanceof DropColumnEvent) {
            visitor.visit((DropColumnEvent) this);
        } else if (this instanceof DropTableEvent) {
            visitor.visit((DropTableEvent) this);
        } else if (this instanceof RenameColumnEvent) {
            visitor.visit((RenameColumnEvent) this);
        } else if (this instanceof TruncateTableEvent) {
            visitor.visit((TruncateTableEvent) this);
        } else {
            throw new IllegalArgumentException("Unknown schema change event type " + getType());
        }
    }

    default <T, E extends Throwable> T visit(SchemaChangeEventVisitor<T, E> visitor) throws E {
        if (this instanceof AddColumnEvent) {
            return visitor.visit((AddColumnEvent) this);
        } else if (this instanceof AlterColumnTypeEvent) {
            return visitor.visit((AlterColumnTypeEvent) this);
        } else if (this instanceof CreateTableEvent) {
            return visitor.visit((CreateTableEvent) this);
        } else if (this instanceof DropColumnEvent) {
            return visitor.visit((DropColumnEvent) this);
        } else if (this instanceof DropTableEvent) {
            return visitor.visit((DropTableEvent) this);
        } else if (this instanceof RenameColumnEvent) {
            return visitor.visit((RenameColumnEvent) this);
        } else if (this instanceof TruncateTableEvent) {
            return visitor.visit((TruncateTableEvent) this);
        } else {
            throw new IllegalArgumentException("Unknown schema change event type " + getType());
        }
    }
}
