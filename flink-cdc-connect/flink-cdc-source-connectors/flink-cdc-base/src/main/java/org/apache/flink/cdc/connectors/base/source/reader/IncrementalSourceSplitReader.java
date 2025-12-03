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

package org.apache.flink.cdc.connectors.base.source.reader;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.Fetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.flink.cdc.common.utils.Preconditions.checkState;
import static org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

/** Basic class read {@link SourceSplitBase} and return {@link SourceRecord}. */
@Experimental
public class IncrementalSourceSplitReader<C extends SourceConfig>
        implements SplitReader<SourceRecords, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceSplitReader.class);

    private final ArrayDeque<SnapshotSplit> snapshotSplits;
    private final ArrayDeque<StreamSplit> streamSplits;
    private final int subtaskId;

    @Nullable private Fetcher<SourceRecords, SourceSplitBase> currentFetcher;

    @Nullable private IncrementalSourceScanFetcher reusedScanFetcher;
    @Nullable private IncrementalSourceStreamFetcher reusedStreamFetcher;

    @Nullable private String currentSplitId;
    private final DataSourceDialect<C> dataSourceDialect;
    private final C sourceConfig;

    private final IncrementalSourceReaderContext context;
    private final SnapshotPhaseHooks snapshotHooks;

    public IncrementalSourceSplitReader(
            int subtaskId,
            DataSourceDialect<C> dataSourceDialect,
            C sourceConfig,
            IncrementalSourceReaderContext context,
            SnapshotPhaseHooks snapshotHooks) {
        this.subtaskId = subtaskId;
        this.snapshotSplits = new ArrayDeque<>();
        this.streamSplits = new ArrayDeque<>(1);
        this.dataSourceDialect = dataSourceDialect;
        this.sourceConfig = sourceConfig;
        this.context = context;
        this.snapshotHooks = snapshotHooks;
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {
        try {
            suspendStreamReaderIfNeed();
            return pollSplitRecords();
        } catch (Exception e) {
            LOG.warn("fetch data failed.", e);
            throw new IOException(e);
        }
    }

    /** Suspends stream reader until updated stream split join again. */
    private void suspendStreamReaderIfNeed() throws Exception {
        if (currentFetcher != null
                && currentFetcher instanceof IncrementalSourceStreamFetcher
                && context.isStreamSplitReaderSuspended()
                && !currentFetcher.isFinished()) {
            ((IncrementalSourceStreamFetcher) currentFetcher).stopReadTask();
            LOG.info("Suspend stream reader to wait the stream split update.");
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        for (SourceSplitBase split : splitsChanges.splits()) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split.asSnapshotSplit());
            } else {
                streamSplits.add(split.asStreamSplit());
            }
        }
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        closeScanFetcher();
        closeStreamFetcher();
    }

    private ChangeEventRecords pollSplitRecords() throws InterruptedException {
        Iterator<SourceRecords> dataIt = null;
        if (currentFetcher == null) {
            // (1) Reads stream split firstly and then read snapshot split
            if (!streamSplits.isEmpty()) {
                // the stream split may come from:
                // (a) the initial stream split
                // (b) added back stream-split in newly added table process
                StreamSplit nextSplit = streamSplits.poll();
                submitStreamSplit(nextSplit);
            } else if (!snapshotSplits.isEmpty()) {
                submitSnapshotSplit(snapshotSplits.poll());
            } else {
                LOG.info("No available split to read.");
            }

            if (currentFetcher != null) {
                dataIt = currentFetcher.pollSplitRecords();
            } else {
                currentSplitId = null;
            }
            return dataIt == null ? finishedSplit(true) : forUnfinishedRecords(dataIt);
        } else if (currentFetcher instanceof IncrementalSourceScanFetcher) {
            dataIt = currentFetcher.pollSplitRecords();
            if (dataIt != null) {
                // first fetch data of snapshot split, return and emit the records of snapshot split
                return forUnfinishedRecords(dataIt);
            } else {
                // (2) try to switch to stream split reading util current snapshot split finished
                ChangeEventRecords finishedRecords;
                if (context.isHasAssignedStreamSplit()) {
                    finishedRecords = forNewAddedTableFinishedSplit(currentSplitId);
                    closeScanFetcher();
                    closeStreamFetcher();
                } else {
                    finishedRecords = finishedSplit(false);
                    SnapshotSplit nextSplit = snapshotSplits.poll();
                    if (nextSplit != null) {
                        checkState(reusedScanFetcher != null);
                        submitSnapshotSplit(nextSplit);
                    } else {
                        closeScanFetcher();
                    }
                }
                return finishedRecords;
            }
        } else if (currentFetcher instanceof IncrementalSourceStreamFetcher) {
            // (3) switch to snapshot split reading if there are newly added snapshot splits
            dataIt = currentFetcher.pollSplitRecords();
            if (dataIt != null) {
                // try to switch to read snapshot split if there are new added snapshot
                SnapshotSplit nextSplit = snapshotSplits.poll();
                if (nextSplit != null) {
                    closeStreamFetcher();
                    LOG.info("It's turn to switch next fetch reader to snapshot split reader");
                    submitSnapshotSplit(nextSplit);
                }
                return ChangeEventRecords.forRecords(STREAM_SPLIT_ID, dataIt);
            } else {
                // null will be returned after receiving suspend stream event
                // finish current stream split reading
                closeStreamFetcher();
                return finishedSplit(true);
            }
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    @VisibleForTesting
    public boolean canAssignNextSplit() {
        return currentFetcher == null || currentFetcher.isFinished();
    }

    private ChangeEventRecords finishedSplit(boolean recycleScanFetcher) {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        if (recycleScanFetcher) {
            closeScanFetcher();
        }
        currentSplitId = null;
        return finishedRecords;
    }

    /**
     * Finishes new added snapshot split, mark the stream split as finished too, we will add the
     * stream split back in {@code MySqlSourceReader}.
     */
    private ChangeEventRecords forNewAddedTableFinishedSplit(final String splitId) {
        final Set<String> finishedSplits = new HashSet<>();
        finishedSplits.add(splitId);
        finishedSplits.add(STREAM_SPLIT_ID);
        currentSplitId = null;
        return new ChangeEventRecords(splitId, Collections.emptyIterator(), finishedSplits);
    }

    private ChangeEventRecords forUnfinishedRecords(Iterator<SourceRecords> dataIt) {
        return ChangeEventRecords.forRecords(currentSplitId, dataIt);
    }

    private void submitSnapshotSplit(SnapshotSplit snapshotSplit) {
        currentSplitId = snapshotSplit.splitId();
        currentFetcher = getScanFetcher();
        FetchTask<SourceSplitBase> fetchTask = dataSourceDialect.createFetchTask(snapshotSplit);
        ((AbstractScanFetchTask) fetchTask).setSnapshotPhaseHooks(snapshotHooks);
        currentFetcher.submitTask(fetchTask);
    }

    private void submitStreamSplit(StreamSplit streamSplit) {
        currentSplitId = streamSplit.splitId();
        currentFetcher = getStreamFetcher();
        FetchTask<SourceSplitBase> fetchTask = dataSourceDialect.createFetchTask(streamSplit);
        currentFetcher.submitTask(fetchTask);
    }

    private IncrementalSourceScanFetcher getScanFetcher() {
        if (reusedScanFetcher == null) {
            reusedScanFetcher =
                    new IncrementalSourceScanFetcher(
                            dataSourceDialect.createFetchTaskContext(sourceConfig), subtaskId);
        }
        return reusedScanFetcher;
    }

    private IncrementalSourceStreamFetcher getStreamFetcher() {
        if (reusedStreamFetcher == null) {
            reusedStreamFetcher =
                    new IncrementalSourceStreamFetcher(
                            dataSourceDialect.createFetchTaskContext(sourceConfig), subtaskId);
        }
        return reusedStreamFetcher;
    }

    private void closeScanFetcher() {
        if (reusedScanFetcher != null) {
            LOG.debug("Close snapshot reader {}", reusedScanFetcher.getClass().getCanonicalName());
            reusedScanFetcher.close();
            if (currentFetcher == reusedScanFetcher) {
                currentFetcher = null;
            }
            reusedScanFetcher = null;
        }
    }

    private void closeStreamFetcher() {
        if (reusedStreamFetcher != null) {
            LOG.debug("Close stream reader {}", reusedStreamFetcher.getClass().getCanonicalName());
            reusedStreamFetcher.close();
            if (currentFetcher == reusedStreamFetcher) {
                currentFetcher = null;
            }
            reusedStreamFetcher = null;
        }
    }
}
