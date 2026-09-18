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

package org.apache.flink.cdc.connectors.tidb.source.reader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import org.apache.flink.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.apache.flink.cdc.connectors.tidb.metrics.TiDBSourceMetrics;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;
import org.apache.flink.cdc.connectors.tidb.table.StartupMode;
import org.apache.flink.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Reads snapshot events then CDC events for a single {@link TiKVKeyRangeSplit}. Runtime region
 * split/merge is handled by {@link CDCClient} inside the assigned key range.
 */
@Internal
public class TiKVSourceReader<T> implements SourceReader<T, TiKVKeyRangeSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(TiKVSourceReader.class);
    private static final int CHANGE_EVENT_BATCH = 1000;
    private static final long STREAMING_VERSION_START_EPOCH = 0L;

    private final SourceReaderContext context;
    private final TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private final TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private final TiConfiguration tiConf;
    private final StartupMode startupMode;

    private TiSession session;
    private TiDBSourceMetrics sourceMetrics;
    private TiKVKeyRangeSplit assignedSplit;
    private Coprocessor.KeyRange keyRange;
    private CDCClient cdcClient;
    private ReaderOutput<T> currentOutput;

    private long resolvedTs = TiKVKeyRangeSplit.NO_RESOLVED_TS;
    private TreeMap<RowKeyWithTs, Cdcpb.Event.Row> prewrites;
    private TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits;
    private boolean cdcStarted;
    private volatile boolean running = true;
    private CompletableFuture<Void> availability;

    public TiKVSourceReader(
            SourceReaderContext context,
            TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            StartupMode startupMode) {
        this.context = context;
        this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
        this.changeEventDeserializationSchema = changeEventDeserializationSchema;
        this.tiConf = tiConf;
        this.startupMode = startupMode;
        this.availability = new CompletableFuture<>();
    }

    @Override
    public void start() {
        session = TiSession.create(tiConf);
        sourceMetrics = new TiDBSourceMetrics(context.metricGroup());
        sourceMetrics.registerMetrics();
        prewrites = new TreeMap<>();
        commits = new TreeMap<>();
        maybeCreateCdcClient();
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        this.currentOutput = output;
        if (!running) {
            return InputStatus.END_OF_INPUT;
        }
        if (assignedSplit == null) {
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
            }
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (assignedSplit.isEmpty()) {
            if (resolvedTs == TiKVKeyRangeSplit.NO_RESOLVED_TS) {
                resolvedTs = STREAMING_VERSION_START_EPOCH;
            }
            return idle();
        }
        if (!cdcStarted) {
            if (startupMode == StartupMode.INITIAL && !assignedSplit.snapshotCompleted()) {
                readSnapshotEvents(output);
            } else if (!assignedSplit.snapshotCompleted()) {
                LOG.info("Skip snapshot read for split {}", assignedSplit.splitId());
                resolvedTs = session.getTimestamp().getVersion();
            }
            startCdc();
            return InputStatus.MORE_AVAILABLE;
        }
        boolean emitted = pollChangeEvents(output);
        return emitted ? InputStatus.MORE_AVAILABLE : idle();
    }

    private InputStatus idle() {
        availability = new CompletableFuture<>();
        CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS)
                .execute(() -> availability.complete(null));
        return InputStatus.NOTHING_AVAILABLE;
    }

    private void startCdc() {
        if (cdcStarted || assignedSplit.isEmpty()) {
            cdcStarted = true;
            return;
        }
        LOG.info("Start CDC for split {} from resolvedTs {}", assignedSplit.splitId(), resolvedTs);
        cdcClient.start(resolvedTs);
        cdcStarted = true;
    }

    private void readSnapshotEvents(ReaderOutput<T> output) throws Exception {
        LOG.info("Read snapshot events for split {}", assignedSplit.splitId());
        final ReaderOutputCollector<T> collector = new ReaderOutputCollector<>(output);
        try (KVClient scanClient = session.createKVClient()) {
            long startTs = session.getTimestamp().getVersion();
            ByteString start = keyRange.getStart();
            while (running) {
                final List<Kvrpcpb.KvPair> segment =
                        scanClient.scan(start, keyRange.getEnd(), startTs);
                if (segment.isEmpty()) {
                    resolvedTs = startTs;
                    break;
                }
                for (final Kvrpcpb.KvPair pair : segment) {
                    if (TableKeyRangeUtils.isRecordKey(pair.getKey().toByteArray())) {
                        snapshotEventDeserializationSchema.deserialize(pair, collector);
                        reportMetrics(0L, startTs);
                    }
                }
                start =
                        RowKey.toRawKey(segment.get(segment.size() - 1).getKey())
                                .next()
                                .toByteString();
            }
        }
    }

    private boolean pollChangeEvents(ReaderOutput<T> output) throws Exception {
        boolean emitted = false;
        for (int i = 0; i < CHANGE_EVENT_BATCH; i++) {
            final Cdcpb.Event.Row row = cdcClient.get();
            if (row == null) {
                break;
            }
            handleRow(row);
        }
        if (cdcClient != null) {
            try {
                resolvedTs = Math.max(resolvedTs, cdcClient.getMaxResolvedTs());
            } catch (Exception e) {
                LOG.debug(
                        "resolvedTs not available yet for split {}: {}",
                        assignedSplit.splitId(),
                        e.getMessage());
            }
        }
        if (!commits.isEmpty()) {
            emitted = flushRows(resolvedTs, output);
        }
        return emitted;
    }

    private void handleRow(final Cdcpb.Event.Row row) {
        if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            return;
        }
        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                LOG.warn("Unsupported row type: {}", row.getType());
        }
    }

    private boolean flushRows(final long timestamp, ReaderOutput<T> output) throws Exception {
        if (output == null) {
            return false;
        }
        boolean emitted = false;
        final ReaderOutputCollector<T> collector = new ReaderOutputCollector<>(output);
        while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
            final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
            final Cdcpb.Event.Row prewriteRow = prewrites.remove(RowKeyWithTs.ofStart(commitRow));
            if (prewriteRow == null) {
                continue;
            }
            changeEventDeserializationSchema.deserialize(prewriteRow, collector);
            reportMetrics(prewriteRow.getStartTs(), commitRow.getCommitTs());
            emitted = true;
        }
        return emitted;
    }

    @Override
    public List<TiKVKeyRangeSplit> snapshotState(long checkpointId) {
        if (assignedSplit == null) {
            return Collections.emptyList();
        }
        try {
            if (currentOutput != null
                    && !commits.isEmpty()
                    && resolvedTs >= STREAMING_VERSION_START_EPOCH) {
                flushRows(resolvedTs, currentOutput);
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to flush CDC rows before checkpoint", e);
        }
        LOG.info(
                "Snapshot reader checkpoint {} for split {} at resolvedTs {}",
                checkpointId,
                assignedSplit.splitId(),
                resolvedTs);
        return Collections.singletonList(assignedSplit.withResolvedTs(resolvedTs));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    @Override
    public void addSplits(List<TiKVKeyRangeSplit> splits) {
        Preconditions.checkState(
                assignedSplit == null,
                "TiDB source reader currently supports exactly one key-range split, but already has "
                        + assignedSplit);
        Preconditions.checkArgument(
                splits.size() == 1,
                "TiDB source reader currently supports exactly one key-range split, got "
                        + splits.size());
        assignedSplit = splits.get(0);
        keyRange = assignedSplit.toKeyRange();
        resolvedTs = assignedSplit.getResolvedTs();
        maybeCreateCdcClient();
        LOG.info(
                "Reader subtask {} received {}, resolvedTs={}",
                context.getIndexOfSubtask(),
                assignedSplit,
                resolvedTs);
        availability.complete(null);
    }

    private void maybeCreateCdcClient() {
        if (cdcClient == null
                && session != null
                && assignedSplit != null
                && !assignedSplit.isEmpty()) {
            cdcClient = new CDCClient(session, keyRange);
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        if (assignedSplit == null) {
            LOG.warn(
                    "Subtask {} received no-more-splits without a key-range split",
                    context.getIndexOfSubtask());
        }
        availability.complete(null);
    }

    @Override
    public void close() throws Exception {
        running = false;
        availability.complete(null);
        if (cdcClient != null) {
            cdcClient.close();
        }
        if (session != null) {
            session.close();
        }
    }

    private void reportMetrics(long messageTs, long fetchTs) {
        long now = System.currentTimeMillis();
        sourceMetrics.recordProcessTime(now);
        long messageTimestamp = TiTimestamp.extractPhysical(messageTs);
        long fetchTimestamp = TiTimestamp.extractPhysical(fetchTs);
        if (messageTimestamp > 0L) {
            if (fetchTimestamp >= messageTimestamp) {
                sourceMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
            sourceMetrics.recordEmitDelay(now - messageTimestamp);
        }
    }

    private static final class ReaderOutputCollector<T> implements Collector<T> {
        private final ReaderOutput<T> output;

        private ReaderOutputCollector(ReaderOutput<T> output) {
            this.output = output;
        }

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {}
    }

    private static final class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs) thatObj;
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getStartTs(), RowKey.decode(row.getKey().toByteArray()));
        }

        static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getCommitTs(), RowKey.decode(row.getKey().toByteArray()));
        }
    }
}
