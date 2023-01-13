/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.base.source.reader;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/** Basic class read {@link SourceSplitBase} and return {@link SourceRecord}. */
@Experimental
public class IncrementalSourceSplitReader<C extends SourceConfig>
        implements SplitReader<SourceRecords, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceSplitReader.class);
    private final Queue<SourceSplitBase> splits;
    private final int subtaskId;

    @Nullable private Fetcher<SourceRecords, SourceSplitBase> currentFetcher;
    @Nullable private String currentSplitId;
    private final DataSourceDialect<C> dataSourceDialect;
    private final C sourceConfig;

    public IncrementalSourceSplitReader(
            int subtaskId, DataSourceDialect<C> dataSourceDialect, C sourceConfig) {
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.dataSourceDialect = dataSourceDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {
        checkSplitOrStartNext();
        Iterator<SourceRecords> dataIt = null;
        try {
            dataIt = currentFetcher.pollSplitRecords();
        } catch (InterruptedException e) {
            LOG.warn("fetch data failed.", e);
            throw new IOException(e);
        }
        return dataIt == null
                ? finishedSnapshotSplit()
                : ChangeEventRecords.forRecords(currentSplitId, dataIt);
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
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentFetcher != null) {
            LOG.info("Close current fetcher {}", currentFetcher.getClass().getCanonicalName());
            currentFetcher.close();
            currentSplitId = null;
        }
    }

    protected void checkSplitOrStartNext() throws IOException {
        // the stream fetcher should keep alive
        if (currentFetcher instanceof IncrementalSourceStreamFetcher) {
            return;
        }

        if (canAssignNextSplit()) {
            final SourceSplitBase nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining.");
            }
            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currentFetcher == null) {
                    final FetchTask.Context taskContext =
                            dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                    currentFetcher = new IncrementalSourceScanFetcher(taskContext, subtaskId);
                }
            } else {
                // point from snapshot split to stream split
                if (currentFetcher != null) {
                    LOG.info("It's turn to read stream split, close current snapshot fetcher.");
                    currentFetcher.close();
                }
                final FetchTask.Context taskContext =
                        dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                currentFetcher = new IncrementalSourceStreamFetcher(taskContext, subtaskId);
                LOG.info("Stream fetcher is created.");
            }
            currentFetcher.submitTask(dataSourceDialect.createFetchTask(nextSplit));
        }
    }

    @VisibleForTesting
    public boolean canAssignNextSplit() {
        return currentFetcher == null || currentFetcher.isFinished();
    }

    private ChangeEventRecords finishedSnapshotSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
