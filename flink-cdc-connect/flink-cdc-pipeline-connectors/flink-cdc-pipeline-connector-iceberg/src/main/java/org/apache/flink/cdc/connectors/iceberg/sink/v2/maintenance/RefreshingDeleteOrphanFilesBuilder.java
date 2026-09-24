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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;
import static org.apache.flink.cdc.common.utils.Preconditions.checkState;

/**
 * Refreshes snapshots used by Iceberg 1.10's ListMetadataFiles operator. Without this, expiration
 * leaves the operator reading deleted manifests and subsequent commits' metadata is not protected.
 */
class RefreshingDeleteOrphanFilesBuilder extends DeleteOrphanFiles.Builder {
    private TableLoader refreshingLoader;

    @Override
    protected TableLoader tableLoader() {
        if (refreshingLoader == null) {
            refreshingLoader = new RefreshingTableLoader(super.tableLoader());
        }
        return refreshingLoader;
    }

    static final class RefreshingTableLoader implements TableLoader {
        private static final long serialVersionUID = 1L;
        private final TableLoader delegate;

        RefreshingTableLoader(TableLoader delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() {
            delegate.open();
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public Table loadTable() {
            Table table = delegate.loadTable();
            checkArgument(
                    table instanceof BaseTable,
                    "Iceberg orphan cleanup requires a catalog returning BaseTable: %s",
                    table.name());
            return new SnapshotRefreshingTable((BaseTable) table);
        }

        @Override
        public TableLoader clone() {
            return new RefreshingTableLoader(delegate.clone());
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class SnapshotRefreshingTable extends BaseTable {
        private static final long serialVersionUID = 1L;

        private SnapshotRefreshingTable(BaseTable table) {
            super(table.operations(), table.name(), table.reporter());
        }

        @Override
        public Iterable<Snapshot> snapshots() {
            refresh();
            checkState(
                    PropertyUtil.propertyAsBoolean(
                            properties(),
                            TableProperties.GC_ENABLED,
                            TableProperties.GC_ENABLED_DEFAULT),
                    "Orphan cleanup is disabled by gc.enabled for table " + name());
            Iterable<Snapshot> snapshots = super.snapshots();
            // Iceberg 1.10 lists table metadata inside its snapshot loop. An empty loop would
            // incorrectly mark the table's own metadata as orphaned. Abort cleanup until a commit.
            checkState(
                    snapshots.iterator().hasNext(),
                    "Orphan cleanup is deferred until table "
                            + name()
                            + " has a committed snapshot.");
            return snapshots;
        }
    }
}
