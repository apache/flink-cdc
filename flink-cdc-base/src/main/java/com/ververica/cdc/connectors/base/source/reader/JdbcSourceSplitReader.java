/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

    @Nullable private Fetcher<SourceRecord, SourceSplitBase> currenFetcher;
    @Nullable private String currentSplitId;
    private JdbcDataSourceDialect dataSourceDialect;

    public JdbcSourceSplitReader(int subtaskId, JdbcDataSourceDialect dataSourceDialect) {
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.dataSourceDialect = dataSourceDialect;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        checkSplitOrStartNext();
        Iterator<SourceRecord> dataIt = null;
        try {
            dataIt = currenFetcher.pollSplitRecords();
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
        if (currenFetcher != null) {
            LOG.info("Close current fetcher {}", currenFetcher.getClass().getCanonicalName());
            currenFetcher.close();
            currentSplitId = null;
        }
    }

    protected void checkSplitOrStartNext() throws IOException {
        // the binlog fetcher should keep alive
        if (currenFetcher instanceof JdbcSourceStreamFetcher) {
            return;
        }

        if (canAssignNextSplit()) {
            final SourceSplitBase nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining.");
            }
            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currenFetcher == null) {
                    final JdbcSourceFetchTaskContext taskContext =
                            dataSourceDialect.createFetchTaskContext(nextSplit);
                    currenFetcher = new JdbcSourceScanFetcher(taskContext, subtaskId);
                }
            } else {
                // point from snapshot split to binlog split
                if (currenFetcher != null) {
                    LOG.info("It's turn to read binlog split, close current snapshot fetcher.");
                    currenFetcher.close();
                }
                final JdbcSourceFetchTaskContext taskContext =
                        dataSourceDialect.createFetchTaskContext(nextSplit);
                currenFetcher = new JdbcSourceStreamFetcher(taskContext, subtaskId);
                LOG.info("Stream fetcher is created.");
            }
            currenFetcher.submitTask(dataSourceDialect.createFetchTask(nextSplit));
        }
    }

    private boolean canAssignNextSplit() {
        return currenFetcher == null || currenFetcher.isFinished();
    }

    private ChangeEventRecords finishedSnapshotSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
