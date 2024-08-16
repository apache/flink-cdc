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

/** Schema change events on table-level. */
public interface TableSchemaChangeEvent extends SchemaChangeEvent {

    default void visit(TableSchemaChangeEventVisitorVoid visitor) {
        try {
            if (this instanceof CreateTableEvent) {
                visitor.visit((CreateTableEvent) this);
            } else if (this instanceof DropTableEvent) {
                visitor.visit((DropTableEvent) this);
            } else if (this instanceof TruncateTableEvent) {
                visitor.visit((TruncateTableEvent) this);
            } else {
                throw new IllegalArgumentException("Unknown schema change event type " + getType());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    default <T> T visit(TableSchemaChangeEventVisitor<T> visitor) {
        try {
            if (this instanceof CreateTableEvent) {
                return visitor.visit((CreateTableEvent) this);
            } else if (this instanceof DropTableEvent) {
                return visitor.visit((DropTableEvent) this);
            } else if (this instanceof TruncateTableEvent) {
                return visitor.visit((TruncateTableEvent) this);
            } else {
                throw new IllegalArgumentException("Unknown schema change event type " + getType());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
