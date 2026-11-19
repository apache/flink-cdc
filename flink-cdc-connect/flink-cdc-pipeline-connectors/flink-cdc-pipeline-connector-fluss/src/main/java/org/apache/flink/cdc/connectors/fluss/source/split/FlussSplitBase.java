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

package org.apache.flink.cdc.connectors.fluss.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

/**
 * Abstract base class for all Fluss source splits. Each split corresponds to a single bucket of a
 * single table, optionally within a specific partition.
 *
 * <p>Concrete subclasses include:
 *
 * <ul>
 *   <li>{@link FlussLogSplit}: reads change log records from a starting offset.
 *   <li>{@link FlussSnapshotSplit}: reads a bounded KV snapshot.
 *   <li>{@link FlussHybridSnapshotLogSplit}: first reads a KV snapshot, then switches to reading
 *       change log from the snapshot's log offset.
 * </ul>
 */
public abstract class FlussSplitBase implements SourceSplit {

    protected final PhysicalTablePath tablePath;
    protected final TableBucket tableBucket;

    /**
     * The schema ID of the last record processed by this split. {@code null} when created by the
     * enumerator (unknown until records are read).
     */
    protected final @Nullable Integer schemaId;

    /**
     * The {@link RowType} corresponding to {@link #schemaId}. Used to restore the deserializer's
     * schema cache on recovery.
     */
    protected final @Nullable RowType rowType;

    protected FlussSplitBase(PhysicalTablePath tablePath, TableBucket tableBucket) {
        this(tablePath, tableBucket, null, null);
    }

    protected FlussSplitBase(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            @Nullable Integer schemaId,
            @Nullable RowType rowType) {
        this.tablePath = tablePath;
        this.tableBucket = tableBucket;
        this.schemaId = schemaId;
        this.rowType = rowType;
    }

    @Override
    public String splitId() {
        if (tablePath.getPartitionName() != null) {
            return tablePath.getDatabaseName()
                    + "."
                    + tablePath.getTableName()
                    + "."
                    + tablePath.getPartitionName()
                    + "."
                    + tableBucket.getBucket();
        }
        return tablePath.getDatabaseName()
                + "."
                + tablePath.getTableName()
                + "."
                + tableBucket.getBucket();
    }

    public PhysicalTablePath getPhysicalTablePath() {
        return tablePath;
    }

    /** Convenience method returning the logical {@link TablePath} (database + table name). */
    public TablePath getTablePath() {
        return tablePath.getTablePath();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public @Nullable Integer getSchemaId() {
        return schemaId;
    }

    public @Nullable RowType getRowType() {
        return rowType;
    }

    public boolean isLogSplit() {
        return this instanceof FlussLogSplit;
    }

    public boolean isSnapshotSplit() {
        return this instanceof FlussSnapshotSplit;
    }

    public boolean isHybridSnapshotLogSplit() {
        return this instanceof FlussHybridSnapshotLogSplit;
    }

    public FlussLogSplit asLogSplit() {
        return (FlussLogSplit) this;
    }

    public FlussSnapshotSplit asSnapshotSplit() {
        return (FlussSnapshotSplit) this;
    }

    public FlussHybridSnapshotLogSplit asHybridSnapshotLogSplit() {
        return (FlussHybridSnapshotLogSplit) this;
    }
}
