/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.state;

import org.apache.flink.api.connector.sink2.StatefulSink;

import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Use to persistent the relationship of TableId and Schema by {@link
 * StatefulSink.StatefulSinkWriter}.
 */
public class TableSchemaState {

    @Nonnull private final TableId tableId;

    @Nonnull private final Schema schema;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link TableSchemaStateSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    public TableSchemaState(TableId tableId, Schema schema) {
        this.tableId = tableId;
        this.schema = schema;
    }

    public TableId getTableId() {
        return tableId;
    }

    public Schema getSchema() {
        return schema;
    }

    @Nullable
    public byte[] getSerializedFormCache() {
        return serializedFormCache;
    }

    public void setSerializedFormCache(@Nullable byte[] serializedFormCache) {
        this.serializedFormCache = serializedFormCache;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSchemaState that = (TableSchemaState) o;
        return tableId.equals(that.tableId) && schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schema);
    }

    @Override
    public String toString() {
        return "TableSchemaState{" + "tableId=" + tableId + ", schema=" + schema + '}';
    }
}
