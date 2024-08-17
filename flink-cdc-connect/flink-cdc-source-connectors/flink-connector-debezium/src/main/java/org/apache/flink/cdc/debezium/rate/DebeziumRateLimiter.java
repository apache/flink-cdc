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

/**
 * The DebeziumRateLimiter interface defines a contract for a rate limiter that can be used to
 * control the rate at which events are processed.
 */
public interface DebeziumRateLimiter {

    /**
     * Attempts to acquire permission to process an event. This method blocks until permission is
     * available, or until the thread is interrupted.
     *
     * @return a double value representing the time in seconds that the caller should wait before
     *     attempting to acquire again. This is useful for implementing a backoff strategy that
     *     gradually increases the wait time between attempts.
     */
    double acquire();

    /**
     * Checks whether the rate limiter is enabled. This can be useful for dynamically enabling or
     * disabling rate limiting based on certain conditions.
     *
     * @return true if the rate limiter is enabled, false otherwise.
     */
    boolean isEnable();

    /**
     * @param isSingleReader when binlog snapshot is end,reader is single Resets the rate limiter to
     *     the initial configuration. This method is useful if the rate limit needs to be
     *     reconfigured dynamically.
     */
    void resetRate(boolean isSingleReader);
}
