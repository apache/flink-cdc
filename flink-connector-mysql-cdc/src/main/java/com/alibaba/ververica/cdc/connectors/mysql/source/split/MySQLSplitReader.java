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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySQLParallelSource;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.getBinaryClient;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.getConnection;

/** The {@link SplitReader} implementation for the {@link MySQLParallelSource}. */
public class MySQLSplitReader implements SplitReader<SourceRecord, MySQLSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSplitReader.class);
    private final Queue<MySQLSplit> splits;
    private final Configuration config;
    private final int subtaskId;

    @Nullable private DebeziumReader<SourceRecord, MySQLSplit> currentReader;
    @Nullable private String currentSplitId;

    public MySQLSplitReader(Configuration config, int subtaskId) {
        this.config = config;
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        checkSplitOrStartNext();
        Iterator<SourceRecord> dataIt = null;
        try {
            dataIt = currentReader.pollSplitRecords();
        } catch (InterruptedException e) {
            LOGGER.warn("fetch data failed.", e);
            throw new IOException(e);
        }
        return dataIt == null
                ? finishedSnapshotSplit()
                : MySQLRecords.forRecords(currentSplitId, dataIt);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySQLSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOGGER.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            LOGGER.info(
                    "Close current debezium reader {}",
                    currentReader.getClass().getCanonicalName());
            currentReader.close();
            currentSplitId = null;
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        // the binlog reader should keep alive
        if (currentReader != null && currentReader instanceof BinlogSplitReader) {
            return;
        }

        if (canAssignNextSplit()) {
            final MySQLSplit nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining");
            }
            currentSplitId = nextSplit.getSplitId();

            if (nextSplit.getSplitKind() == MySQLSplitKind.SNAPSHOT) {
                if (currentReader == null) {
                    final MySqlConnection jdbcConnection = getConnection(config);
                    final BinaryLogClient binaryLogClient = getBinaryClient(config);
                    final StatefulTaskContext statefulTaskContext =
                            new StatefulTaskContext(config, binaryLogClient, jdbcConnection);
                    currentReader = new SnapshotSplitReader(statefulTaskContext, subtaskId);
                }
                currentReader.submitSplit(nextSplit);
            } else {
                // point from snapshot split to binlog split
                if (currentReader != null) {
                    LOGGER.info("It's turn to read binlog split, close current snapshot reader");
                    currentReader.close();
                }
                final MySqlConnection jdbcConnection = getConnection(config);
                final BinaryLogClient binaryLogClient = getBinaryClient(config);
                final StatefulTaskContext statefulTaskContext =
                        new StatefulTaskContext(config, binaryLogClient, jdbcConnection);
                LOGGER.info("Create binlog reader");
                currentReader = new BinlogSplitReader(statefulTaskContext, subtaskId);
                currentReader.submitSplit(nextSplit);
            }
        }
    }

    private boolean canAssignNextSplit() {
        return currentReader == null || currentReader.isIdle();
    }

    private MySQLRecords finishedSnapshotSplit() {
        final MySQLRecords finishedRecords = MySQLRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }
}
