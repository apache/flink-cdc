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

package com.ververica.cdc.debezium.rate;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.RateLimiter;

/** An implementation of {@link RateLimiter} based on Guava's RateLimiter. */
public class GuavaRateLimiter implements com.ververica.cdc.debezium.rate.RateLimiter {

    /** Max rate of total tasks per second. */
    private final long maxPerSecond;

    /** Guava's RateLimiter. */
    private RateLimiter rateLimiter;

    public GuavaRateLimiter(long maxPerSecond, int numParallelism) {
        this.maxPerSecond = maxPerSecond;
        resetRate(numParallelism);
    }

    @Override
    public void resetRate(int numParallelism) {
        final float maxPerSecondPerSubtask = (float) maxPerSecond / numParallelism;
        this.rateLimiter = RateLimiter.create(maxPerSecondPerSubtask);
    }

    @Override
    public int acquire() {
        return (int) (1000 * rateLimiter.acquire());
    }
}
