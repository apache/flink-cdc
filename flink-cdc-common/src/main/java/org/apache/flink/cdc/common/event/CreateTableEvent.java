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
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.DataSource;

import java.util.Objects;

/**
 * A {@link SchemaChangeEvent} that represents an {@code CREATE TABLE} DDL. this will be sent by
 * {@link DataSource} before all {@link DataChangeEvent} with the same tableId
 */
@PublicEvolving
public class CreateTableEvent implements SchemaChangeEvent {

    private static final long serialVersionUID = 1L;

    private final TableId tableId;

    private final Schema schema;

    public CreateTableEvent(TableId tableId, Schema schema) {
        this.tableId = tableId;
        this.schema = schema;
    }

    /** Returns the table schema. */
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CreateTableEvent)) {
            return false;
        }
        CreateTableEvent that = (CreateTableEvent) o;
        return Objects.equals(tableId, that.tableId) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schema);
    }

    @Override
    public String toString() {
        return "CreateTableEvent{" + "tableId=" + tableId + ", schema=" + schema + '}';
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    @Override
    public SchemaChangeEventType getType() {
        return SchemaChangeEventType.CREATE_TABLE;
    }

    @Override
    public SchemaChangeEvent copy(TableId newTableId) {
        return new CreateTableEvent(newTableId, schema);
    }
}
