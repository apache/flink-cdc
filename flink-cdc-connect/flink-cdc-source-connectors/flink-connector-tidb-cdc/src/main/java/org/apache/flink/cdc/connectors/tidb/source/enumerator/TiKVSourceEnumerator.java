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

package org.apache.flink.cdc.connectors.tidb.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;
import org.apache.flink.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Coprocessor.KeyRange;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Enumerator that splits a TiDB table by TiKV regions <b>once</b> and assigns a contiguous {@link
 * TiKVKeyRangeSplit} to each reader.
 *
 * <p>Restore reuses the checkpointed splits and never queries PD for a new region layout.
 */
@Internal
public class TiKVSourceEnumerator
        implements SplitEnumerator<TiKVKeyRangeSplit, TiKVEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(TiKVSourceEnumerator.class);

    private final SplitEnumeratorContext<TiKVKeyRangeSplit> context;
    @Nullable private final TiConfiguration tiConf;
    private final String database;
    private final String tableName;

    private final List<TiKVKeyRangeSplit> unassignedSplits;
    private final TreeSet<Integer> readersAwaitingSplit;
    private boolean enumerated;
    private int parallelism;

    public TiKVSourceEnumerator(
            SplitEnumeratorContext<TiKVKeyRangeSplit> context,
            TiConfiguration tiConf,
            String database,
            String tableName) {
        this(
                context,
                tiConf,
                database,
                tableName,
                new TiKVEnumeratorState(new ArrayList<>(), false, -1));
    }

    public TiKVSourceEnumerator(
            SplitEnumeratorContext<TiKVKeyRangeSplit> context,
            @Nullable TiConfiguration tiConf,
            String database,
            String tableName,
            TiKVEnumeratorState checkpoint) {
        this.context = context;
        this.tiConf = tiConf;
        this.database = database;
        this.tableName = tableName;
        this.unassignedSplits = new ArrayList<>(checkpoint.getUnassignedSplits());
        this.enumerated = checkpoint.isEnumerated();
        this.parallelism = checkpoint.getParallelism();
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @VisibleForTesting
    static TiKVSourceEnumerator forRestoredSplits(
            SplitEnumeratorContext<TiKVKeyRangeSplit> context,
            List<TiKVKeyRangeSplit> unassigned,
            int parallelism) {
        return new TiKVSourceEnumerator(
                context,
                null,
                "db",
                "table",
                new TiKVEnumeratorState(unassigned, true, parallelism));
    }

    @Override
    public void start() {
        if (enumerated) {
            if (parallelism > 0 && parallelism != context.currentParallelism()) {
                throw new FlinkRuntimeException(
                        String.format(
                                "TiDB CDC does not support changing source parallelism after splits are assigned. checkpoint parallelism=%s, current=%s",
                                parallelism, context.currentParallelism()));
            }
            LOG.info(
                    "Restore TiDB enumerator for {}.{}, {} unassigned split(s), skip region discovery",
                    database,
                    tableName,
                    unassignedSplits.size());
            return;
        }
        discoverSplits();
        enumerated = true;
        assignSplits();
    }

    private void discoverSplits() {
        if (tiConf == null) {
            throw new FlinkRuntimeException(
                    "TiConfiguration is required to split table " + database + "." + tableName);
        }
        this.parallelism = context.currentParallelism();
        try (TiSession session = TiSession.create(tiConf)) {
            TiTableInfo tableInfo = session.getCatalog().getTable(database, tableName);
            if (tableInfo == null) {
                throw new FlinkRuntimeException(
                        String.format("Table %s.%s does not exist.", database, tableName));
            }
            long tableId = tableInfo.getId();
            List<KeyRange> ranges =
                    TableKeyRangeUtils.getTableKeyRangesByRegion(session, tableId, parallelism);
            unassignedSplits.clear();
            for (int i = 0; i < ranges.size(); i++) {
                unassignedSplits.add(TiKVKeyRangeSplit.fromKeyRange(splitId(i), ranges.get(i)));
            }
            LOG.info(
                    "Discovered {} region-based key-range split(s) for {}.{}, tableId={}, parallelism={}",
                    unassignedSplits.size(),
                    database,
                    tableName,
                    tableId,
                    parallelism);
        } catch (FlinkRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to split table %s.%s by TiKV regions. The job will fail rather than assign overlapping or empty ranges.",
                            database, tableName),
                    e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            return;
        }
        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<TiKVKeyRangeSplit> splits, int subtaskId) {
        LOG.info("Add {} split(s) back from subtask {}", splits.size(), subtaskId);
        unassignedSplits.addAll(splits);
        if (context.registeredReaders().containsKey(subtaskId)) {
            readersAwaitingSplit.add(subtaskId);
        }
        assignSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        // Wait for the reader to request a split.
    }

    private void assignSplits() {
        if (!enumerated) {
            return;
        }
        final Iterator<Integer> awaiting = readersAwaitingSplit.iterator();
        while (awaiting.hasNext()) {
            int reader = awaiting.next();
            if (!context.registeredReaders().containsKey(reader)) {
                awaiting.remove();
                continue;
            }
            Optional<TiKVKeyRangeSplit> next = takeSplitForSubtask(reader);
            if (next.isPresent()) {
                TiKVKeyRangeSplit split = next.get();
                LOG.info("Assign {} to subtask {}", split, reader);
                context.assignSplit(split, reader);
                awaiting.remove();
            } else {
                LOG.info("No more splits for subtask {}", reader);
                context.signalNoMoreSplits(reader);
                awaiting.remove();
            }
        }
    }

    private Optional<TiKVKeyRangeSplit> takeSplitForSubtask(int subtaskId) {
        final String expectedId = splitId(subtaskId);
        Iterator<TiKVKeyRangeSplit> iterator = unassignedSplits.iterator();
        while (iterator.hasNext()) {
            TiKVKeyRangeSplit split = iterator.next();
            if (expectedId.equals(split.splitId())) {
                iterator.remove();
                return Optional.of(split);
            }
        }
        return Optional.empty();
    }

    static String splitId(int subtaskId) {
        return "tidb-" + subtaskId;
    }

    @Override
    public TiKVEnumeratorState snapshotState(long checkpointId) {
        LOG.info(
                "Enumerator snapshot checkpoint {} with enumerated={}, unassigned={}",
                checkpointId,
                enumerated,
                unassignedSplits.size());
        return new TiKVEnumeratorState(new ArrayList<>(unassignedSplits), enumerated, parallelism);
    }

    @Override
    public void close() throws IOException {
        // TiSession is closed after discovery.
    }

    @VisibleForTesting
    List<TiKVKeyRangeSplit> getUnassignedSplits() {
        return unassignedSplits;
    }

    @VisibleForTesting
    boolean isEnumerated() {
        return enumerated;
    }
}
