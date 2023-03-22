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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlRecords;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import io.debezium.connector.mysql.MySqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createBinaryClient;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;
import static com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;

/** The {@link SplitReader} implementation for the {@link MySqlSource}. */
public class MySqlSplitReader implements SplitReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitReader.class);
    private final ArrayDeque<MySqlSnapshotSplit> snapshotSplits;
    private final ArrayDeque<MySqlBinlogSplit> binlogSplits;
    private final MySqlSourceConfig sourceConfig;
    private final int subtaskId;
    private final MySqlSourceReaderContext context;

    @Nullable private String currentSplitId;
    @Nullable private DebeziumReader<SourceRecords, MySqlSplit> currentReader;
    @Nullable private SnapshotSplitReader reusedSnapshotReader;
    @Nullable private BinlogSplitReader reusedBinlogReader;

    public MySqlSplitReader(
            MySqlSourceConfig sourceConfig, int subtaskId, MySqlSourceReaderContext context) {
        this.sourceConfig = sourceConfig;
        this.subtaskId = subtaskId;
        this.snapshotSplits = new ArrayDeque<>();
        this.binlogSplits = new ArrayDeque<>(1);
        this.context = context;
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {
        try {
            suspendBinlogReaderIfNeed();
            return pollSplitRecords();
        } catch (InterruptedException e) {
            LOG.warn("fetch data failed.", e);
            throw new IOException(e);
        }
    }

    /** Suspends binlog reader until updated binlog split join again. */
    private void suspendBinlogReaderIfNeed() {
        if (currentReader != null
                && currentReader instanceof BinlogSplitReader
                && context.isBinlogSplitReaderSuspended()
                && !currentReader.isFinished()) {
            ((BinlogSplitReader) currentReader).stopBinlogReadTask();
            LOG.info("Suspend binlog reader to wait the binlog split update.");
        }
    }

    private MySqlRecords pollSplitRecords() throws InterruptedException {
        Iterator<SourceRecords> dataIt;
        if (currentReader == null) {
            // (1) Reads binlog split firstly and then read snapshot split
            if (binlogSplits.size() > 0) {
                // the binlog split may come from:
                // (a) the initial binlog split
                // (b) added back binlog-split in newly added table process
                MySqlSplit nextSplit = binlogSplits.poll();
                currentSplitId = nextSplit.splitId();
                currentReader = getBinlogSplitReader();
                currentReader.submitSplit(nextSplit);
            } else if (snapshotSplits.size() > 0) {
                MySqlSplit nextSplit = snapshotSplits.poll();
                currentSplitId = nextSplit.splitId();
                currentReader = getSnapshotSplitReader();
                currentReader.submitSplit(nextSplit);
            } else {
                LOG.info("No available split to read.");
            }
            dataIt = currentReader.pollSplitRecords();
            return dataIt == null ? finishedSplit() : forRecords(dataIt);
        } else if (currentReader instanceof SnapshotSplitReader) {
            // (2) try to switch to binlog split reading util current snapshot split finished
            dataIt = currentReader.pollSplitRecords();
            if (dataIt != null) {
                // first fetch data of snapshot split, return and emit the records of snapshot split
                MySqlRecords records;
                if (context.isHasAssignedBinlogSplit()) {
                    records = forNewAddedTableFinishedSplit(currentSplitId, dataIt);
                    closeSnapshotReader();
                    closeBinlogReader();
                } else {
                    records = forRecords(dataIt);
                    MySqlSplit nextSplit = snapshotSplits.poll();
                    if (nextSplit != null) {
                        currentSplitId = nextSplit.splitId();
                        currentReader.submitSplit(nextSplit);
                    } else {
                        closeSnapshotReader();
                    }
                }
                return records;
            } else {
                return finishedSplit();
            }
        } else if (currentReader instanceof BinlogSplitReader) {
            // (3) switch to snapshot split reading if there are newly added snapshot splits
            dataIt = currentReader.pollSplitRecords();
            if (dataIt != null) {
                // try to switch to read snapshot split if there are new added snapshot
                MySqlSplit nextSplit = snapshotSplits.poll();
                if (nextSplit != null) {
                    closeBinlogReader();
                    LOG.info("It's turn to switch next fetch reader to snapshot split reader");
                    currentSplitId = nextSplit.splitId();
                    currentReader = getSnapshotSplitReader();
                    currentReader.submitSplit(nextSplit);
                }
                return MySqlRecords.forBinlogRecords(BINLOG_SPLIT_ID, dataIt);
            } else {
                // null will be returned after receiving suspend binlog event
                // finish current binlog split reading
                closeBinlogReader();
                return finishedSplit();
            }
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    private MySqlRecords finishedSplit() {
        final MySqlRecords finishedRecords = MySqlRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }

    private MySqlRecords forRecords(Iterator<SourceRecords> dataIt) {
        if (currentReader instanceof SnapshotSplitReader) {
            final MySqlRecords finishedRecords =
                    MySqlRecords.forSnapshotRecords(currentSplitId, dataIt);
            closeSnapshotReader();
            return finishedRecords;
        } else {
            return MySqlRecords.forBinlogRecords(currentSplitId, dataIt);
        }
    }

    /**
     * Finishes new added snapshot split, mark the binlog split as finished too, we will add the
     * binlog split back in {@code MySqlSourceReader}.
     */
    private MySqlRecords forNewAddedTableFinishedSplit(
            final String splitId, final Iterator<SourceRecords> recordsForSplit) {
        final Set<String> finishedSplits = new HashSet<>();
        finishedSplits.add(splitId);
        finishedSplits.add(BINLOG_SPLIT_ID);
        currentSplitId = null;
        return new MySqlRecords(splitId, recordsForSplit, finishedSplits);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.info("Handling split change {}", splitsChanges);
        for (MySqlSplit mySqlSplit : splitsChanges.splits()) {
            if (mySqlSplit.isSnapshotSplit()) {
                snapshotSplits.add(mySqlSplit.asSnapshotSplit());
            } else {
                binlogSplits.add(mySqlSplit.asBinlogSplit());
            }
        }
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        closeSnapshotReader();
        closeBinlogReader();
    }

    private SnapshotSplitReader getSnapshotSplitReader() {
        if (reusedSnapshotReader == null) {
            final MySqlConnection jdbcConnection = createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            reusedSnapshotReader = new SnapshotSplitReader(statefulTaskContext, subtaskId);
        }
        return reusedSnapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader() {
        if (reusedBinlogReader == null) {
            final MySqlConnection jdbcConnection = createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                    createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                    new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            reusedBinlogReader = new BinlogSplitReader(statefulTaskContext, subtaskId);
        }
        return reusedBinlogReader;
    }

    private void closeSnapshotReader() {
        if (reusedSnapshotReader != null) {
            LOG.debug(
                    "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            if (reusedSnapshotReader == currentReader) {
                currentReader = null;
            }
            reusedSnapshotReader = null;
        }
    }

    private void closeBinlogReader() {
        if (reusedBinlogReader != null) {
            LOG.debug("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            if (reusedBinlogReader == currentReader) {
                currentReader = null;
            }
            reusedBinlogReader = null;
        }
    }
}
