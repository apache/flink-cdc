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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;
import com.ververica.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import com.ververica.cdc.connectors.base.source.reader.external.JdbcSourceScanFetcher;
import com.ververica.cdc.connectors.base.source.reader.external.JdbcSourceStreamFetcher;
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
public class JdbcSourceSplitReader implements SplitReader<SourceRecord, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitReader.class);
    private final Queue<SourceSplitBase> splits;
    private final int subtaskId;

    @Nullable private Fetcher<SourceRecord, SourceSplitBase> currentFetcher;
    @Nullable private String currentSplitId;
    private final JdbcDataSourceDialect dataSourceDialect;
    private final JdbcSourceConfig sourceConfig;

    public JdbcSourceSplitReader(
            int subtaskId, JdbcDataSourceDialect dataSourceDialect, JdbcSourceConfig sourceConfig) {
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.dataSourceDialect = dataSourceDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        checkSplitOrStartNext();
        Iterator<SourceRecord> dataIt = null;
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
        // the binlog fetcher should keep alive
        if (currentFetcher instanceof JdbcSourceStreamFetcher) {
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
                    final JdbcSourceFetchTaskContext taskContext =
                            dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                    currentFetcher = new JdbcSourceScanFetcher(taskContext, subtaskId);
                }
            } else {
                // point from snapshot split to binlog split
                if (currentFetcher != null) {
                    LOG.info("It's turn to read binlog split, close current snapshot fetcher.");
                    currentFetcher.close();
                }
                final JdbcSourceFetchTaskContext taskContext =
                        dataSourceDialect.createFetchTaskContext(nextSplit, sourceConfig);
                currentFetcher = new JdbcSourceStreamFetcher(taskContext, subtaskId);
                LOG.info("Stream fetcher is created.");
            }
            currentFetcher.submitTask(dataSourceDialect.createFetchTask(nextSplit));
        }
    }

    private boolean canAssignNextSplit() {
        return currentFetcher == null || currentFetcher.isFinished();
    }

    private ChangeEventRecords finishedSnapshotSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
