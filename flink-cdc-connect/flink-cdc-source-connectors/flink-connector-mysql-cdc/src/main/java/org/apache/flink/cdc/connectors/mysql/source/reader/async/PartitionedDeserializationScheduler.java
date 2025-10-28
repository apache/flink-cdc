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

package org.apache.flink.cdc.connectors.mysql.source.reader.async;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Partitioned parallel deserialization scheduler: single-reader, multi-worker; strong ordering per
 * primary key; replay on the source thread; advance offsets after replay.
 */
public class PartitionedDeserializationScheduler<T> implements AsyncScheduler<T> {

    private static final int MAX_DRAIN_PER_PARTITION_PER_ROUND = 64;

    private final DebeziumDeserializationSchema<T> deserializer;
    private final int deserPoolSize;
    private final int partitionWorkers;
    private final int emitPoolSize; // reserved, currently unused
    private final int queueCapacity;

    private volatile ExecutorService deserExecutor;
    private volatile ExecutorService[] partitionExecutors;
    private volatile BlockingQueue<Batch<T>>[] partitionQueues;

    private final AtomicInteger globalSequence = new AtomicInteger(0);
    private volatile int nextGlobalToEmit = 0;
    private final java.util.concurrent.ConcurrentHashMap<Integer, Batch<T>> globalReadyBatches =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Object globalEmissionLock = new Object();

    public PartitionedDeserializationScheduler(
            DebeziumDeserializationSchema<T> deserializer,
            int deserPoolSize,
            int partitionWorkers,
            int emitPoolSize,
            int queueCapacity) {
        this.deserializer = Objects.requireNonNull(deserializer);
        this.deserPoolSize = Math.max(0, deserPoolSize);
        this.partitionWorkers = Math.max(0, partitionWorkers);
        this.emitPoolSize = Math.max(0, emitPoolSize);
        this.queueCapacity = Math.max(1, queueCapacity);
    }

    @Override
    public boolean isEnabled() {
        return deserPoolSize > 0 || partitionWorkers > 0;
    }

    @Override
    public void schedulePartitioned(SourceRecord record, AtomicInteger pendingTasks) {
        ensurePartitionExecutors();
        int pid = computePartitionIndex(record);
        pendingTasks.incrementAndGet();
        partitionExecutors[pid].execute(
                () -> {
                    Batch<T> batch = deserializeToBatch(record);
                    BlockingQueue<Batch<T>> q = partitionQueues[pid];
                    handleEnqueue(q, batch);
                    pendingTasks.decrementAndGet();
                });
    }

    @Override
    public void drainRound(
            SourceOutput<T> output, java.util.function.Consumer<BinlogOffset> onAfterEmit) {
        // Replay global batches in strict submission order
        synchronized (globalEmissionLock) {
            while (true) {
                Batch<T> batch = globalReadyBatches.remove(nextGlobalToEmit);
                if (batch == null) break;
                emitBatch(batch, output);
                if (onAfterEmit != null && batch.lastOffset != null)
                    onAfterEmit.accept(batch.lastOffset);
                nextGlobalToEmit++;
            }
        }
        // Round-robin drain per-partition queues
        if (partitionQueues != null) {
            for (BlockingQueue<Batch<T>> q : partitionQueues) {
                Batch<T> batch;
                int polled = 0;
                while ((batch = q.poll()) != null && polled < MAX_DRAIN_PER_PARTITION_PER_ROUND) {
                    emitBatch(batch, output);
                    if (onAfterEmit != null && batch.lastOffset != null)
                        onAfterEmit.accept(batch.lastOffset);
                    polled++;
                }
            }
        }
    }

    @Override
    public void waitAndDrainAll(
            SourceOutput<T> output,
            AtomicInteger pendingTasks,
            java.util.function.Consumer<BinlogOffset> onAfterEmit)
            throws InterruptedException {
        while (pendingTasks.get() > 0) {
            drainRound(output, onAfterEmit);
            Thread.sleep(1L);
        }
        drainRound(output, onAfterEmit);
    }

    // ---------------- helpers ----------------
    private Batch<T> deserializeToBatch(SourceRecord element) {
        try {
            // One sourceRecord may contain multiple records, need to deserialize it into a list.
            final List<T> list = new ArrayList<>(1);
            Collector<T> c =
                    new Collector<T>() {
                        @Override
                        public void collect(T record) {
                            list.add(record);
                        }

                        @Override
                        public void close() {}
                    };
            deserializer.deserialize(element, c);
            BinlogOffset offset = RecordUtils.getBinlogPosition(element);
            if (list.isEmpty()) {
                return Batch.empty(offset);
            } else if (list.size() == 1) {
                return Batch.single(list.get(0), offset);
            } else {
                return Batch.multiple(list, offset);
            }
        } catch (Exception e) {
            throw new RuntimeException("Async deserialization failed", e);
        }
    }

    private void emitBatch(Batch<T> batch, SourceOutput<T> output) {
        if (batch.singleRecord != null) {
            output.collect(batch.singleRecord);
            return;
        }
        if (batch.records == null || batch.records.isEmpty()) {
            return;
        }
        Object[] snapshot = batch.records.toArray();
        for (Object o : snapshot) {
            @SuppressWarnings("unchecked")
            T t = (T) o;
            output.collect(t);
        }
    }

    private void ensureDeserExecutor() {
        if (deserExecutor == null && deserPoolSize > 0) {
            synchronized (this) {
                if (deserExecutor == null) {
                    deserExecutor =
                            Executors.newFixedThreadPool(
                                    deserPoolSize, new NamedThreadFactory("mysql-cdc-deser"));
                }
            }
        }
    }

    private void ensurePartitionExecutors() {
        if (partitionWorkers <= 0) return;
        if (partitionExecutors == null || partitionQueues == null) {
            synchronized (this) {
                if (partitionExecutors == null) {
                    partitionExecutors = new ExecutorService[partitionWorkers];
                    for (int i = 0; i < partitionWorkers; i++) {
                        partitionExecutors[i] =
                                Executors.newSingleThreadExecutor(
                                        new NamedThreadFactory("mysql-cdc-pkworker-" + i));
                    }
                }
                if (partitionQueues == null) {
                    @SuppressWarnings("unchecked")
                    BlockingQueue<Batch<T>>[] qs =
                            (BlockingQueue<Batch<T>>[]) new BlockingQueue<?>[partitionWorkers];
                    for (int i = 0; i < partitionWorkers; i++)
                        qs[i] = new ArrayBlockingQueue<>(queueCapacity);
                    partitionQueues = qs;
                }
            }
        }
    }

    private int computePartitionIndex(SourceRecord record) {
        Object key = record.key();
        int h = (key == null) ? 0 : key.hashCode();
        h ^= (h >>> 16);
        int idx = h % partitionWorkers;
        return idx < 0 ? -idx : idx;
    }

    private void handleEnqueue(BlockingQueue<Batch<T>> q, Batch<T> payload) {
        try {
            q.put(payload);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while enqueuing partition payload", ie);
        }
    }

    private static final class Batch<X> {
        final X singleRecord; // fast-path for single element
        final List<X> records; // used for multi-element batches
        final BinlogOffset lastOffset;

        private Batch(X singleRecord, List<X> records, BinlogOffset lastOffset) {
            this.singleRecord = singleRecord;
            this.records = records;
            this.lastOffset = lastOffset;
        }

        static <X> Batch<X> single(X one, BinlogOffset offset) {
            return new Batch<>(one, null, offset);
        }

        static <X> Batch<X> multiple(List<X> many, BinlogOffset offset) {
            return new Batch<>(null, many, offset);
        }

        static <X> Batch<X> empty(BinlogOffset offset) {
            return new Batch<>(null, java.util.Collections.emptyList(), offset);
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger idx = new AtomicInteger(1);

        private NamedThreadFactory(String prefix) {
            this.prefix = Objects.requireNonNull(prefix);
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + '-' + idx.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
