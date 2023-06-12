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

package com.ververica.cdc.connectors.oceanbase.source;

import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** OceanBase Snapshot Chunk Reader. */
public class OceanBaseSnapshotChunkSplitter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseSnapshotChunkSplitter.class);

    private final OceanBaseDataSource dataSource;
    private final int readerNum;
    private final OceanBaseDialect dialect;
    private final BlockingQueue<OceanBaseSnapshotChunkReader> chunkReaders;
    private final ExecutorService executor;
    private final CompletionService<Boolean> completionService;
    private final AtomicBoolean waiting = new AtomicBoolean();
    private transient volatile Exception exception;
    private transient volatile boolean closed = false;

    public OceanBaseSnapshotChunkSplitter(
            OceanBaseDataSource dataSource,
            int readerNum,
            OceanBaseDialect dialect,
            ReadCompleteListener readCompleteListener) {
        this.dataSource = dataSource;
        this.readerNum = readerNum;
        this.dialect = dialect;
        this.chunkReaders = new LinkedBlockingQueue<>(readerNum * 2);
        this.executor = Executors.newFixedThreadPool(readerNum);
        this.completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < readerNum; i++) {
            this.completionService.submit(
                    () -> {
                        while (!this.closed) {
                            if (waiting.get() && chunkReaders.isEmpty()) {
                                return true;
                            }
                            try {
                                OceanBaseSnapshotChunkReader chunk =
                                        chunkReaders.poll(1000, TimeUnit.MILLISECONDS);
                                if (chunk != null) {
                                    chunk.read(dataSource);
                                    readCompleteListener.accept(chunk);
                                }
                            } catch (Exception e) {
                                this.exception = e;
                                return false;
                            }
                        }
                        return true;
                    });
        }
    }

    public void split(
            @Nonnull String dbName,
            @Nonnull String tableName,
            List<String> chunkKeyColumns,
            @Nonnull OceanBaseSnapshotChunkBound startChunkBound,
            int chunkSize,
            JdbcConnection.ResultSetConsumer resultSetConsumer)
            throws Exception {

        if (chunkKeyColumns == null || chunkKeyColumns.isEmpty()) {
            checkException();
            chunkReaders.put(
                    new OceanBaseSnapshotChunkReader(
                            dialect,
                            dbName,
                            tableName,
                            0,
                            chunkKeyColumns,
                            OceanBaseSnapshotChunkBound.START_BOUND,
                            OceanBaseSnapshotChunkBound.END_BOUND,
                            Integer.MIN_VALUE,
                            resultSetConsumer));
            return;
        }

        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            OceanBaseSnapshotChunkBound lowerBound;
            OceanBaseSnapshotChunkBound upperBound = startChunkBound;

            int chunkId = 0;
            do {
                checkException();
                String sql =
                        dialect.getQueryNewChunkBoundSql(
                                dbName,
                                tableName,
                                chunkKeyColumns,
                                upperBound.getValue(),
                                chunkSize);
                LOG.info("Execute query chunk bound sql: " + sql);
                ResultSet rs = statement.executeQuery(sql);
                List<Object> result;
                if (rs.next()) {
                    result = new ArrayList<>();
                    for (String keyColumn : chunkKeyColumns) {
                        result.add(rs.getObject(keyColumn));
                    }
                } else {
                    result = null;
                }
                lowerBound = upperBound;
                upperBound =
                        result == null
                                ? OceanBaseSnapshotChunkBound.END_BOUND
                                : OceanBaseSnapshotChunkBound.middleOf(result);
                chunkReaders.put(
                        new OceanBaseSnapshotChunkReader(
                                dialect,
                                dbName,
                                tableName,
                                chunkId++,
                                chunkKeyColumns,
                                lowerBound,
                                upperBound,
                                chunkSize,
                                resultSetConsumer));
            } while (!OceanBaseSnapshotChunkBound.END_BOUND.equals(upperBound));
        }
    }

    public void checkException() {
        if (exception != null) {
            throw new RuntimeException("Chunk reader exception", exception);
        }
    }

    public void waitTermination() throws InterruptedException {
        this.waiting.set(true);
        for (int i = 0; i < readerNum; i++) {
            completionService.take();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                }
            }
        }
    }

    interface ReadCompleteListener {
        void accept(OceanBaseSnapshotChunkReader chunkReader) throws Exception;
    }
}
