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

import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBFetchTaskContext;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBScanFetcher;
import com.ververica.cdc.connectors.mongodb.source.reader.fetch.MongoDBStreamFetcher;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/** The {@link SplitReader} implementation for the {@link MongoDBSource}. */
public class MongoDBSourceSplitReader
        implements SplitReader<SourceRecord, SourceSplitBase<CollectionId, CollectionSchema>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceSplitReader.class);
    private final Queue<SourceSplitBase<CollectionId, CollectionSchema>> splits;

    private final MongoDBDialect dialect;
    private final MongoDBSourceConfig sourceConfig;
    private final MongoDBSourceReaderContext context;
    private final int subtaskId;

    @Nullable private String currentSplitId;

    @Nullable
    private Fetcher<SourceRecord, SourceSplitBase<CollectionId, CollectionSchema>> currenFetcher;

    public MongoDBSourceSplitReader(
            MongoDBDialect dialect,
            MongoDBSourceConfig sourceConfig,
            int subtaskId,
            MongoDBSourceReaderContext context) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
        this.context = context;
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
    public void handleSplitsChanges(
            SplitsChange<SourceSplitBase<CollectionId, CollectionSchema>> splitsChange) {
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
    public void wakeUp() {}

    @Override
    public void close() {
        if (currenFetcher != null) {
            LOG.info("Close current split fetcher {}", currenFetcher.getClass().getCanonicalName());
            currenFetcher.close();
            currentSplitId = null;
        }
    }

    protected void checkSplitOrStartNext() throws IOException {
        // the stream fetcher should keep alive
        if (currenFetcher instanceof MongoDBStreamFetcher) {
            return;
        }

        if (canAssignNextSplit()) {
            final SourceSplitBase<CollectionId, CollectionSchema> nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining.");
            }
            currentSplitId = nextSplit.splitId();

            if (nextSplit.isSnapshotSplit()) {
                if (currenFetcher == null) {
                    final MongoDBFetchTaskContext taskContext =
                            dialect.createFetchTaskContext(nextSplit);
                    currenFetcher = new MongoDBScanFetcher(taskContext, subtaskId);
                }
            } else {
                // point from snapshot split to stream split
                if (currenFetcher != null) {
                    LOG.info("It's turn to read stream split, close current snapshot fetcher.");
                    currenFetcher.close();
                }
                final MongoDBFetchTaskContext taskContext =
                        dialect.createFetchTaskContext(nextSplit);
                currenFetcher = new MongoDBStreamFetcher(taskContext, subtaskId);
                LOG.info("Stream fetcher is created.");
            }
            currenFetcher.submitTask(dialect.createFetchTask(nextSplit));
        }
    }

    public boolean canAssignNextSplit() {
        return currenFetcher == null || currenFetcher.isFinished();
    }

    private ChangeEventRecords finishedSnapshotSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
