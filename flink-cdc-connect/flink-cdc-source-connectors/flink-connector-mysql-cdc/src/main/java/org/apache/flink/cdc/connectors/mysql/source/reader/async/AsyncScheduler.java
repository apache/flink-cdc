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

import org.apache.kafka.connect.source.SourceRecord;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Parallel scheduler interface: performs thread-safe in-source deserialization and advances offsets
 * when replaying on the source thread.
 */
public interface AsyncScheduler<T> {

    /** Whether parallelization is enabled (false = fall back to single-threaded path). */
    boolean isEnabled();

    /**
     * Schedule data-change events by primary-key partition; pendingTasks counts the number of tasks
     * that are enqueued but not yet replayed.
     */
    void schedulePartitioned(SourceRecord record, AtomicInteger pendingTasks);

    /** Schedule global async work (e.g., control or non-DML events). */
    void scheduleGlobalAsync(SourceRecord record);

    /** Perform one replay round on the source thread; advance offset via onAfterEmit. */
    void drainRound(SourceOutput<T> output, Consumer<BinlogOffset> onAfterEmit);

    /** Wait until all scheduled tasks have completed and been replayed. */
    void waitAndDrainAll(
            SourceOutput<T> output, AtomicInteger pendingTasks, Consumer<BinlogOffset> onAfterEmit)
            throws InterruptedException;
}
