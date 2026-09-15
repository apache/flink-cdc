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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** Binds real table metadata on first use, after CDC has created the target. */
final class DeferredTableLoader implements TableLoader {
    private static final long serialVersionUID = 1L;
    private final TableLoader delegate;
    private final String tableName;
    private transient BaseTable loaded;
    private transient Table facade;
    private transient boolean opened;

    DeferredTableLoader(TableLoader delegate, String tableName) {
        this.delegate = delegate;
        this.tableName = tableName;
    }

    @Override
    public void open() {
        opened = true;
    }

    @Override
    public boolean isOpen() {
        return opened;
    }

    @Override
    public Table loadTable() {
        if (facade == null) {
            facade = new DeferredTable(this, tableName);
        }
        return facade;
    }

    Table loadDeletionTable() {
        return new DeferredTable(this, tableName, new DeferredFileIO(this));
    }

    synchronized BaseTable actualTable() {
        if (loaded == null) {
            if (!delegate.isOpen()) {
                delegate.open();
            }
            Table table = delegate.loadTable();
            checkArgument(
                    table instanceof BaseTable,
                    "CDC maintenance requires a catalog returning BaseTable: %s",
                    tableName);
            loaded = (BaseTable) table;
        }
        return loaded;
    }

    boolean isReady() {
        try {
            actualTable();
            return true;
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    @Override
    public DeferredTableLoader clone() {
        return new DeferredTableLoader(delegate.clone(), tableName);
    }

    @Override
    public void close() throws IOException {
        loaded = null;
        facade = null;
        opened = false;
        delegate.close();
    }

    private static final class DeferredTable extends BaseTable {
        private static final long serialVersionUID = 1L;
        private final DeferredTableLoader loader;
        private final FileIO deletionIO;

        private DeferredTable(DeferredTableLoader loader, String name) {
            this(loader, name, null);
        }

        private DeferredTable(DeferredTableLoader loader, String name, FileIO deletionIO) {
            super(new DeferredOperations(loader), name);
            this.loader = loader;
            this.deletionIO = deletionIO;
        }

        @Override
        public FileIO io() {
            return deletionIO == null ? super.io() : deletionIO;
        }

        @Override
        public void refresh() {
            if (loader.isReady()) {
                super.refresh();
            }
        }

        @Override
        public Snapshot currentSnapshot() {
            return loader.isReady() ? super.currentSnapshot() : null;
        }
    }

    /** No synthetic schema or metadata is exposed while a target is missing. */
    private static final class DeferredOperations implements TableOperations {
        private final DeferredTableLoader loader;
        private final FileIO io;

        private DeferredOperations(DeferredTableLoader loader) {
            this.loader = loader;
            this.io = new DeferredFileIO(loader);
        }

        private TableOperations actual() {
            return loader.actualTable().operations();
        }

        @Override
        public TableMetadata current() {
            return actual().current();
        }

        @Override
        public TableMetadata refresh() {
            return actual().refresh();
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            actual().commit(base, metadata);
        }

        @Override
        public FileIO io() {
            // Scan tasks serialize the actual FileIO class and properties as JSON. Only the
            // submission-time deletion operators may capture the deferred serializable handle.
            return loader.isReady() ? loader.actualTable().io() : io;
        }

        @Override
        public EncryptionManager encryption() {
            return actual().encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return actual().metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return actual().locationProvider();
        }

        @Override
        public TableOperations temp(TableMetadata uncommittedMetadata) {
            return actual().temp(uncommittedMetadata);
        }

        @Override
        public long newSnapshotId() {
            return actual().newSnapshotId();
        }

        @Override
        public boolean requireStrictCleanup() {
            return actual().requireStrictCleanup();
        }
    }

    /** Iceberg captures this serializable FileIO while assembling deletion operators. */
    private static final class DeferredFileIO implements SupportsBulkOperations {
        private static final long serialVersionUID = 1L;
        private final DeferredTableLoader loader;

        private DeferredFileIO(DeferredTableLoader loader) {
            this.loader = loader;
        }

        private FileIO actual() {
            return loader.actualTable().io();
        }

        @Override
        public InputFile newInputFile(String path) {
            return actual().newInputFile(path);
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            return actual().newInputFile(path, length);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return actual().newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            actual().deleteFile(path);
        }

        @Override
        public void deleteFiles(Iterable<String> paths) {
            // Iceberg flushes deletion batches before every checkpoint, including empty batches.
            if (!paths.iterator().hasNext()) {
                return;
            }
            FileIO io = actual();
            checkArgument(
                    io instanceof SupportsBulkOperations,
                    "Maintenance FileIO must support bulk deletion: %s",
                    io.getClass().getName());
            ((SupportsBulkOperations) io).deleteFiles(paths);
        }

        @Override
        public Map<String, String> properties() {
            return actual().properties();
        }

        @Override
        public void close() {
            try {
                loader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
