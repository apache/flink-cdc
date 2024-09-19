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

package org.apache.flink.cdc.connectors.mysql.rate;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * The SnapshotGuavaRateLimiter class implements Flink's RateLimiter interface, which is used to
 * limit the read rate of data sources. It uses Guava's RateLimiter to control the number of
 * requests allowed to pass per second, ensuring that parallel tasks can adjust the rate limit after
 * the checkpoint is completed.
 */
public class MysqlCdcGuavaRateLimiter
        implements org.apache.flink.api.connector.source.util.ratelimit.RateLimiter {

    private final Executor limiter =
            Executors.newSingleThreadExecutor(
                    new ExecutorThreadFactory("flink-snapshot-rate-limiter"));
    private final RateLimiter rateLimiter;

    private int getTokenCountAtOnce;

    public MysqlCdcGuavaRateLimiter(double recordsPerSecond, int parallelism) {
        this.rateLimiter = RateLimiter.create(recordsPerSecond);
        this.getTokenCountAtOnce = parallelism;
    }

    /**
     * Due to the CDC feature, the degree of parallelism will change to 1 after the checkpoint is
     * completed, so you need to reset the speed limiter.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        getTokenCountAtOnce = 1;
    }

    public void getAcquire() {
        rateLimiter.acquire(getTokenCountAtOnce);
    }

    @Override
    public CompletionStage<Void> acquire() {
        return CompletableFuture.runAsync(this::getAcquire, limiter);
    }
}
