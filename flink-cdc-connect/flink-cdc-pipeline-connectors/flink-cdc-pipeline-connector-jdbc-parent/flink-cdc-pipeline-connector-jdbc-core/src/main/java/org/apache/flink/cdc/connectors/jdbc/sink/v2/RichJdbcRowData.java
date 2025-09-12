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

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Enriched {@link JdbcRowData} for serializing {@link org.apache.flink.cdc.common.event.Event}s.
 */
public class RichJdbcRowData implements JdbcRowData {
    protected final TableId tableId;
    protected final RowKind rowKind;
    protected final @Nullable byte[] rows;
    protected final @Nullable Schema schema;
    protected final @Nullable Boolean hasPrimaryKey;

    protected RichJdbcRowData(
            RowKind rowKind, TableId tableId, @Nullable Schema schema, @Nullable byte[] rows) {
        this.tableId = tableId;
        this.schema = schema;
        this.rowKind = rowKind;
        this.rows = rows;

        if (schema != null) {
            this.hasPrimaryKey = !schema.primaryKeys().isEmpty();
        } else {
            this.hasPrimaryKey = null;
        }
    }

    /** Builder clas for {@link RichJdbcRowData}. */
    public static class Builder {
        private TableId tableId;
        private Schema schema;
        private RowKind rowKind;
        private byte[] rows;

        public Builder setTableId(TableId tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder setSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setRowKind(RowKind rowKind) {
            this.rowKind = rowKind;
            return this;
        }

        public Builder setRows(byte[] rows) {
            this.rows = rows;
            return this;
        }

        public RichJdbcRowData build() {
            Preconditions.checkNotNull(rowKind, "No Row Kind provided for JdbcRowData.");
            Preconditions.checkNotNull(tableId, "No Table Id provided for JdbcRowData.");
            return new RichJdbcRowData(rowKind, tableId, schema, rows);
        }
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public byte[] getRows() {
        return rows;
    }

    public boolean hasPrimaryKey() {
        return Boolean.TRUE.equals(hasPrimaryKey);
    }

    @Override
    public String toString() {
        return "RichJdbcRowData{"
                + "tableId="
                + tableId
                + ", schema="
                + schema
                + ", rowKind="
                + rowKind
                + ", rows="
                + (rows != null ? new String(rows) : "null")
                + ", hasPrimaryKey="
                + hasPrimaryKey
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RichJdbcRowData)) {
            return false;
        }

        RichJdbcRowData that = (RichJdbcRowData) o;
        return hasPrimaryKey == that.hasPrimaryKey
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(schema, that.schema)
                && rowKind == that.rowKind
                && Arrays.equals(rows, that.rows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, schema, rowKind, Arrays.hashCode(rows), hasPrimaryKey);
    }
}
