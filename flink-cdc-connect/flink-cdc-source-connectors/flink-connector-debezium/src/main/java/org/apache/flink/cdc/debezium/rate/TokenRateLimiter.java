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

package org.apache.flink.cdc.debezium.rate;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;

import java.io.Serializable;

/**
 * A concrete implementation of the DebeziumRateLimiter interface that uses a token bucket algorithm
 * to limit the rate of event processing. This implementation is based on the Guava RateLimiter.
 */
public class TokenRateLimiter implements DebeziumRateLimiter, Serializable {

    /**
     * The rate limiter instance that controls the rate of event processing. It uses a token bucket
     * algorithm to enforce the rate limit.
     */
    private RateLimiter rateLimiter;

    private final Rate rate;
    private final int parallelism;

    public static DebeziumRateLimiter createZeroRateLimiter() {
        return new TokenRateLimiter(new Rate(0), 1);
    }

    /** Constructs a new TokenRateLimiter with the specified rate configuration. */
    public TokenRateLimiter(Rate rate, int parallelism) {
        this.rate = rate;
        this.parallelism = parallelism;
    }

    /**
     * @param isSingleReader when binlog snapshot is end,reader is single,rate is work for single or
     *     parallel reader Resets the rate limiter to the initial configuration. This method is
     *     useful if the rate limit needs to be reconfigured dynamically.
     */
    public void resetRate(boolean isSingleReader) {
        if (!isEnable()) {
            // if rate is 0,just return and do nothing
        } else if (isSingleReader) {
            this.rateLimiter = RateLimiter.create(rate.getMaxPerCount());
        } else {
            long maxPerCount = rate.getMaxPerCount() / parallelism;
            maxPerCount = maxPerCount > 0 ? maxPerCount : 1;
            this.rateLimiter = RateLimiter.create(maxPerCount);
        }
    }

    @Override
    public double acquire() {
        if (isEnable()) {
            return rateLimiter.acquire();
        }
        return 0;
    }

    /**
     * Checks whether the rate limiter is enabled. The rate limiter is considered enabled if the
     * maximum number of permits per time unit is greater than zero.
     *
     * @return true if the rate limiter is enabled, false otherwise.
     */
    @Override
    public boolean isEnable() {
        return rate.getMaxPerCount() > 0;
    }
}
