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

package com.ververica.cdc.connectors.mongodb.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplit;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static java.util.Collections.singletonList;

/** The {@link SplitReader} implementation for the {@link MongoDBSource}. */
public class MongoDBHybridSplitReader implements MongoDBSplitReader<MongoDBSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBHybridSplitReader.class);
    private final Queue<MongoDBSplit> splits;
    private final MongoDBSourceConfig sourceConfig;
    private final int subtaskId;
    private final MongoDBSourceReaderContext context;

    @Nullable private String currentSplitId;
    @Nullable private MongoDBSplitReader<? extends MongoDBSplit> currentReader;

    public MongoDBHybridSplitReader(
            MongoDBSourceConfig sourceConfig, int subtaskId, MongoDBSourceReaderContext context) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.context = context;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        checkSplitOrStartNext();
        checkNeedStopSteamReader();
        return currentReader.fetch();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoDBSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChange);
        splits.addAll(splitsChange.splits());
    }

    @Override
    public boolean canAssignNextSplit() {
        return currentReader == null || currentReader.canAssignNextSplit();
    }

    @Override
    public void suspend() {
        // do nothing
    }

    @Override
    public void wakeUp() {
        // do nothing
    }

    @Override
    public void close() {
        if (currentReader != null) {
            LOG.info("Close current split reader {}", currentReader.getClass().getCanonicalName());
            currentReader.close();
            currentSplitId = null;
        }
    }

    private void checkSplitOrStartNext() {
        if (canAssignNextSplit()) {
            MongoDBSplit nextSplit = splits.poll();
            if (nextSplit == null) {
                return;
            }

            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currentReader instanceof MongoDBStreamSplitReader) {
                    LOG.info(
                            "This is the point from stream split reading change to snapshot split reading");
                    currentReader.close();
                    currentReader = null;
                }
                if (currentReader == null) {
                    currentReader = new MongoDBSnapshotSplitReader(sourceConfig, subtaskId);
                }
                MongoDBSnapshotSplitReader snapshotSplitReader =
                        (MongoDBSnapshotSplitReader) currentReader;
                snapshotSplitReader.handleSplitsChanges(splitChangeOf(nextSplit.asSnapshotSplit()));
            } else {
                // point from snapshot split to stream split
                if (currentReader != null) {
                    LOG.info("It's turn to read stream split, close current snapshot reader");
                    currentReader.close();
                }

                MongoDBStreamSplitReader streamSplitReader =
                        new MongoDBStreamSplitReader(sourceConfig, subtaskId);
                LOG.info("StreamSplitReader is created.");
                streamSplitReader.handleSplitsChanges(splitChangeOf(nextSplit.asStreamSplit()));
                currentReader = streamSplitReader;
            }
        }
    }

    private void checkNeedStopSteamReader() {
        if (currentReader instanceof MongoDBStreamSplitReader
                && context.needStopStreamSplitReader()
                && !currentReader.canAssignNextSplit()) {
            currentReader.close();
        }
    }

    private <T extends MongoDBSplit> SplitsAddition<T> splitChangeOf(T split) {
        return new SplitsAddition<>(singletonList(split));
    }
}
