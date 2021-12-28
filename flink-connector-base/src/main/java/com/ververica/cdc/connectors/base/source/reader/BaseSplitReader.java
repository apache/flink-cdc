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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.connectors.base.source.config.SourceConfig;
import com.ververica.cdc.connectors.base.source.dialect.SnapshotEventDialect;
import com.ververica.cdc.connectors.base.source.dialect.StreamingEventDialect;
import com.ververica.cdc.connectors.base.source.reader.split.Reader;
import com.ververica.cdc.connectors.base.source.reader.split.SnapshotReader;
import com.ververica.cdc.connectors.base.source.reader.split.StreamReader;
import com.ververica.cdc.connectors.base.source.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.split.SourceSplitBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/** basic class read {@link SourceSplitBase} and return {@link SourceRecord}. */
public class BaseSplitReader implements SplitReader<SourceRecord, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSplitReader.class);
    private final Queue<SourceSplitBase> splits;
    private final SourceConfig sourceConfig;
    private final int subtaskId;

    @Nullable private Reader<SourceRecord, SourceSplitBase> currentReader;
    @Nullable private String currentSplitId;
    private SnapshotEventDialect snapshotEventDialect;
    private StreamingEventDialect streamingEventDialect;

    public BaseSplitReader(
            SourceConfig sourceConfig,
            int subtaskId,
            SnapshotEventDialect snapshotEventDialect,
            StreamingEventDialect streamingEventDialect) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.snapshotEventDialect = snapshotEventDialect;
        this.streamingEventDialect = streamingEventDialect;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        checkSplitOrStartNext();
        Iterator<SourceRecord> dataIt = null;
        try {
            dataIt = currentReader.pollSplitRecords();
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
        if (currentReader != null) {
            LOG.info(
                    "Close current debezium reader {}",
                    currentReader.getClass().getCanonicalName());
            currentReader.close();
            currentSplitId = null;
        }
    }

    protected void checkSplitOrStartNext() throws IOException {
        // the binlog reader should keep alive
        if (currentReader instanceof StreamReader) {
            return;
        }

        if (canAssignNextSplit()) {
            final SourceSplitBase nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining.");
            }
            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currentReader == null) {
                    SnapshotEventDialect.SnapshotContext snapshotContext =
                            snapshotEventDialect.createSnapshotContext(sourceConfig);
                    currentReader =
                            new SnapshotReader(
                                    snapshotContext,
                                    subtaskId,
                                    snapshotEventDialect,
                                    streamingEventDialect);
                }
            } else {
                // point from snapshot split to binlog split
                if (currentReader != null) {
                    LOG.info("It's turn to read binlog split, close current snapshot reader.");
                    currentReader.close();
                }
                // todo instance a StreamReader.

                LOG.info("StreamReader is created.");
            }
            currentReader.submitSplit(nextSplit);
        }
    }

    private boolean canAssignNextSplit() {
        return currentReader == null || currentReader.isFinished();
    }

    private ChangeEventRecords finishedSnapshotSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
