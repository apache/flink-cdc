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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.split.MySqlRecords;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createBinaryClient;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;

/** The {@link SplitReader} implementation for the {@link MySqlSource}. */
public class MySqlSplitReader implements SplitReader<SourceRecord, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitReader.class);
    private final Queue<MySqlSplit> splits;
    private final MySqlConnection jdbcConnection;
    private final BinaryLogClient binaryLogClient;
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executor;

    @Nullable private DebeziumReader<SourceRecord, MySqlSplit> currentReader;
    @Nullable private String currentSplitId;

    public MySqlSplitReader(MySqlSourceConfig sourceConfig, int subtaskId) {
        this.splits = new ArrayDeque<>();
        this.jdbcConnection = createMySqlConnection(sourceConfig.getDbzConfiguration());
        this.binaryLogClient = createBinaryClient(sourceConfig.getDbzConfiguration());
        this.statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subtaskId).build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
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
                : MySqlRecords.forRecords(currentSplitId, dataIt);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySqlSplit> splitsChanges) {
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
        LOG.info("Close MySQL split reader");
        closeCurrentReader();
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
            if (binaryLogClient != null) {
                binaryLogClient.disconnect();
            }
        } catch (Exception e) {
            LOG.error("Close MySQL split reader error", e);
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        // the binlog reader should keep alive
        if (currentReader instanceof BinlogSplitReader) {
            return;
        }

        if (canAssignNextSplit()) {
            final MySqlSplit nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining");
            }
            currentSplitId = nextSplit.splitId();
            // close current reader and then create a new reader to read split
            closeCurrentReader();
            if (nextSplit.isSnapshotSplit()) {
                currentReader =
                        new SnapshotSplitReader(
                                statefulTaskContext, nextSplit.asSnapshotSplit(), executor);
            } else {
                // start to read binlog split
                LOG.info("It's turn to read binlog split, create binlog reader");
                currentReader =
                        new BinlogSplitReader(
                                statefulTaskContext, nextSplit.asBinlogSplit(), executor);
            }
            currentReader.start();
        }
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    private boolean canAssignNextSplit() {
        return currentReader == null || currentReader.isFinished();
    }

    private MySqlRecords finishedSnapshotSplit() {
        final MySqlRecords finishedRecords = MySqlRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
