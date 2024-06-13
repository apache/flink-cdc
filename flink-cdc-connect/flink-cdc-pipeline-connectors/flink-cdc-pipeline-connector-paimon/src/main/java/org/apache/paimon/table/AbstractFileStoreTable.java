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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.metastore.AddPartitionCommitCallback;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.metastore.TagPreviewCommitCallback;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.CallbackUtils;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.DynamicBucketRowKeyExtractor;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.UnawareBucketRowKeyExtractor;
import org.apache.paimon.table.source.InnerStreamTableScan;
import org.apache.paimon.table.source.InnerStreamTableScanImpl;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.InnerTableScanImpl;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.SnapshotReaderImpl;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.tag.TagPreview;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/** Copy from Paimon repo to change the visibility of the class to public. */
public abstract class AbstractFileStoreTable implements FileStoreTable {
    private static final long serialVersionUID = 1L;
    protected final FileIO fileIO;
    protected final Path path;
    protected final TableSchema tableSchema;
    protected final CatalogEnvironment catalogEnvironment;

    protected AbstractFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.path = path;
        if (!tableSchema.options().containsKey(CoreOptions.PATH.key())) {
            Map<String, String> newOptions = new HashMap(tableSchema.options());
            newOptions.put(CoreOptions.PATH.key(), path.toString());
            tableSchema = tableSchema.copy(newOptions);
        }

        this.tableSchema = tableSchema;
        this.catalogEnvironment = catalogEnvironment;
    }

    public Optional<Statistics> statistics() {
        Snapshot latestSnapshot = this.snapshotManager().latestSnapshot();
        return latestSnapshot != null
                ? this.store().newStatsFileHandler().readStats(latestSnapshot)
                : Optional.empty();
    }

    public BucketMode bucketMode() {
        return this.store().bucketMode();
    }

    public CatalogEnvironment catalogEnvironment() {
        return this.catalogEnvironment;
    }

    public RowKeyExtractor createRowKeyExtractor() {
        switch (this.bucketMode()) {
            case FIXED:
                return new FixedBucketRowKeyExtractor(this.schema());
            case DYNAMIC:
            case GLOBAL_DYNAMIC:
                return new DynamicBucketRowKeyExtractor(this.schema());
            case UNAWARE:
                return new UnawareBucketRowKeyExtractor(this.schema());
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + this.bucketMode());
        }
    }

    public SnapshotReader newSnapshotReader() {
        return new SnapshotReaderImpl(
                this.store().newScan(),
                this.tableSchema,
                this.coreOptions(),
                this.snapshotManager(),
                this.splitGenerator(),
                this.nonPartitionFilterConsumer(),
                DefaultValueAssigner.create(this.tableSchema),
                this.store().pathFactory(),
                this.name());
    }

    public InnerTableScan newScan() {
        return new InnerTableScanImpl(
                this.coreOptions(),
                this.newSnapshotReader(),
                this.snapshotManager(),
                DefaultValueAssigner.create(this.tableSchema));
    }

    public InnerStreamTableScan newStreamScan() {
        return new InnerStreamTableScanImpl(
                this.coreOptions(),
                this.newSnapshotReader(),
                this.snapshotManager(),
                this.supportStreamingReadOverwrite(),
                DefaultValueAssigner.create(this.tableSchema));
    }

    protected abstract SplitGenerator splitGenerator();

    protected abstract BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer();

    protected abstract FileStoreTable copy(TableSchema var1);

    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        this.checkImmutability(dynamicOptions);
        return this.copyInternal(dynamicOptions, true);
    }

    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        this.checkImmutability(dynamicOptions);
        return this.copyInternal(dynamicOptions, false);
    }

    public FileStoreTable internalCopyWithoutCheck(Map<String, String> dynamicOptions) {
        return this.copyInternal(dynamicOptions, true);
    }

    private void checkImmutability(Map<String, String> dynamicOptions) {
        Map<String, String> options = this.tableSchema.options();
        dynamicOptions.forEach(
                (k, v) -> {
                    if (!Objects.equals(v, options.get(k))) {
                        SchemaManager.checkAlterTableOption(k);
                    }
                });
    }

    private FileStoreTable copyInternal(Map<String, String> dynamicOptions, boolean tryTimeTravel) {
        Map<String, String> options = new HashMap(this.tableSchema.options());
        dynamicOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        options.remove(k);
                    } else {
                        options.put(k, v);
                    }
                });
        Options newOptions = Options.fromMap(options);
        newOptions.set(CoreOptions.PATH, this.path.toString());
        CoreOptions.setDefaultValues(newOptions);
        TableSchema newTableSchema = this.tableSchema.copy(newOptions.toMap());
        if (tryTimeTravel) {
            newTableSchema = (TableSchema) this.tryTimeTravel(newOptions).orElse(newTableSchema);
        }

        SchemaValidation.validateTableSchema(newTableSchema);
        return this.copy(newTableSchema);
    }

    public FileStoreTable copyWithLatestSchema() {
        Map<String, String> options = this.tableSchema.options();
        SchemaManager schemaManager = new SchemaManager(this.fileIO(), this.location());
        Optional<TableSchema> optionalLatestSchema = schemaManager.latest();
        if (optionalLatestSchema.isPresent()) {
            TableSchema newTableSchema = (TableSchema) optionalLatestSchema.get();
            newTableSchema = newTableSchema.copy(options);
            SchemaValidation.validateTableSchema(newTableSchema);
            return this.copy(newTableSchema);
        } else {
            return this;
        }
    }

    protected SchemaManager schemaManager() {
        return new SchemaManager(this.fileIO(), this.path);
    }

    public CoreOptions coreOptions() {
        return this.store().options();
    }

    public FileIO fileIO() {
        return this.fileIO;
    }

    public Path location() {
        return this.path;
    }

    public TableSchema schema() {
        return this.tableSchema;
    }

    public SnapshotManager snapshotManager() {
        return this.store().snapshotManager();
    }

    public ExpireSnapshots newExpireSnapshots() {
        return new ExpireSnapshotsImpl(
                this.snapshotManager(),
                this.store().newSnapshotDeletion(),
                this.store().newTagManager(),
                this.coreOptions().snapshotExpireCleanEmptyDirectories());
    }

    public TableCommitImpl newCommit(String commitUser) {
        CoreOptions options = this.coreOptions();
        Runnable snapshotExpire = null;
        if (!options.writeOnly()) {
            ExpireSnapshots expireSnapshots =
                    this.newExpireSnapshots()
                            .retainMax(options.snapshotNumRetainMax())
                            .retainMin(options.snapshotNumRetainMin())
                            .maxDeletes(options.snapshotExpireLimit());
            long snapshotTimeRetain = options.snapshotTimeRetain().toMillis();
            snapshotExpire =
                    () -> {
                        expireSnapshots
                                .olderThanMills(System.currentTimeMillis() - snapshotTimeRetain)
                                .expire();
                    };
        }

        return new TableCommitImpl(
                this.store().newCommit(commitUser),
                this.createCommitCallbacks(),
                snapshotExpire,
                options.writeOnly() ? null : this.store().newPartitionExpire(commitUser),
                options.writeOnly() ? null : this.store().newTagCreationManager(),
                this.catalogEnvironment.lockFactory().create(),
                CoreOptions.fromMap(this.options()).consumerExpireTime(),
                new ConsumerManager(this.fileIO, this.path),
                options.snapshotExpireExecutionMode(),
                this.name());
    }

    private List<CommitCallback> createCommitCallbacks() {
        List<CommitCallback> callbacks =
                new ArrayList(CallbackUtils.loadCommitCallbacks(this.coreOptions()));
        CoreOptions options = this.coreOptions();
        MetastoreClient.Factory metastoreClientFactory =
                this.catalogEnvironment.metastoreClientFactory();
        if (options.partitionedTableInMetastore()
                && metastoreClientFactory != null
                && this.tableSchema.partitionKeys().size() > 0) {
            callbacks.add(new AddPartitionCommitCallback(metastoreClientFactory.create()));
        }

        TagPreview tagPreview = TagPreview.create(options);
        if (options.tagToPartitionField() != null
                && tagPreview != null
                && metastoreClientFactory != null
                && this.tableSchema.partitionKeys().isEmpty()) {
            TagPreviewCommitCallback callback =
                    new TagPreviewCommitCallback(
                            new AddPartitionTagCallback(
                                    metastoreClientFactory.create(), options.tagToPartitionField()),
                            tagPreview);
            callbacks.add(callback);
        }

        return callbacks;
    }

    private Optional<TableSchema> tryTimeTravel(Options options) {
        CoreOptions coreOptions = new CoreOptions(options);
        switch (coreOptions.startupMode()) {
            case FROM_SNAPSHOT:
            case FROM_SNAPSHOT_FULL:
                if (coreOptions.scanVersion() != null) {
                    return this.travelToVersion(coreOptions.scanVersion(), options);
                } else {
                    if (coreOptions.scanSnapshotId() != null) {
                        return this.travelToSnapshot(coreOptions.scanSnapshotId(), options);
                    }

                    return this.travelToTag(coreOptions.scanTagName(), options);
                }
            case FROM_TIMESTAMP:
                Snapshot snapshot =
                        StaticFromTimestampStartingScanner.timeTravelToTimestamp(
                                this.snapshotManager(), coreOptions.scanTimestampMills());
                return this.travelToSnapshot(snapshot, options);
            default:
                return Optional.empty();
        }
    }

    private Optional<TableSchema> travelToVersion(String version, Options options) {
        options.remove(CoreOptions.SCAN_VERSION.key());
        if (this.tagManager().tagExists(version)) {
            options.set(CoreOptions.SCAN_TAG_NAME, version);
            return this.travelToTag(version, options);
        } else if (version.chars().allMatch(Character::isDigit)) {
            options.set(CoreOptions.SCAN_SNAPSHOT_ID.key(), version);
            return this.travelToSnapshot(Long.parseLong(version), options);
        } else {
            throw new RuntimeException("Cannot find a time travel version for " + version);
        }
    }

    private Optional<TableSchema> travelToTag(String tagName, Options options) {
        return this.travelToSnapshot(this.tagManager().taggedSnapshot(tagName), options);
    }

    private Optional<TableSchema> travelToSnapshot(long snapshotId, Options options) {
        SnapshotManager snapshotManager = this.snapshotManager();
        return snapshotManager.snapshotExists(snapshotId)
                ? this.travelToSnapshot(snapshotManager.snapshot(snapshotId), options)
                : Optional.empty();
    }

    private Optional<TableSchema> travelToSnapshot(@Nullable Snapshot snapshot, Options options) {
        return snapshot != null
                ? Optional.of(
                        this.schemaManager().schema(snapshot.schemaId()).copy(options.toMap()))
                : Optional.empty();
    }

    public void rollbackTo(long snapshotId) {
        SnapshotManager snapshotManager = this.snapshotManager();
        Preconditions.checkArgument(
                snapshotManager.snapshotExists(snapshotId),
                "Rollback snapshot '%s' doesn't exist.",
                new Object[] {snapshotId});
        this.rollbackHelper().cleanLargerThan(snapshotManager.snapshot(snapshotId));
    }

    public void createTag(String tagName, long fromSnapshotId) {
        SnapshotManager snapshotManager = this.snapshotManager();
        Preconditions.checkArgument(
                snapshotManager.snapshotExists(fromSnapshotId),
                "Cannot create tag because given snapshot #%s doesn't exist.",
                new Object[] {fromSnapshotId});
        this.createTag(tagName, snapshotManager.snapshot(fromSnapshotId));
    }

    public void createTag(String tagName) {
        Snapshot latestSnapshot = this.snapshotManager().latestSnapshot();
        Preconditions.checkNotNull(
                latestSnapshot, "Cannot create tag because latest snapshot doesn't exist.");
        this.createTag(tagName, latestSnapshot);
    }

    private void createTag(String tagName, Snapshot fromSnapshot) {
        this.tagManager().createTag(fromSnapshot, tagName, this.store().createTagCallbacks());
    }

    public void deleteTag(String tagName) {
        this.tagManager().deleteTag(tagName, this.store().newTagDeletion(), this.snapshotManager());
    }

    public void createBranch(String branchName, String tagName) {
        this.branchManager().createBranch(branchName, tagName);
    }

    public void deleteBranch(String branchName) {
        this.branchManager().deleteBranch(branchName);
    }

    public void rollbackTo(String tagName) {
        TagManager tagManager = this.tagManager();
        Preconditions.checkArgument(
                tagManager.tagExists(tagName),
                "Rollback tag '%s' doesn't exist.",
                new Object[] {tagName});
        Snapshot taggedSnapshot = tagManager.taggedSnapshot(tagName);
        this.rollbackHelper().cleanLargerThan(taggedSnapshot);

        try {
            SnapshotManager snapshotManager = this.snapshotManager();
            if (!snapshotManager.snapshotExists(taggedSnapshot.id())) {
                this.fileIO.writeFileUtf8(
                        this.snapshotManager().snapshotPath(taggedSnapshot.id()),
                        this.fileIO.readFileUtf8(tagManager.tagPath(tagName)));
                snapshotManager.commitEarliestHint(taggedSnapshot.id());
            }

        } catch (IOException var5) {
            throw new UncheckedIOException(var5);
        }
    }

    public TagManager tagManager() {
        return new TagManager(this.fileIO, this.path);
    }

    public BranchManager branchManager() {
        return new BranchManager(
                this.fileIO,
                this.path,
                this.snapshotManager(),
                this.tagManager(),
                this.schemaManager());
    }

    private RollbackHelper rollbackHelper() {
        return new RollbackHelper(
                this.snapshotManager(),
                this.tagManager(),
                this.fileIO,
                this.store().newSnapshotDeletion(),
                this.store().newTagDeletion());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            AbstractFileStoreTable that = (AbstractFileStoreTable) o;
            return Objects.equals(this.path, that.path)
                    && Objects.equals(this.tableSchema, that.tableSchema);
        } else {
            return false;
        }
    }
}
