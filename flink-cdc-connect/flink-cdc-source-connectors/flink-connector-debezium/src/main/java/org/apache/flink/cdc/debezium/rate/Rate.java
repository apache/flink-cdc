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

import java.io.Serializable;

/**
 * Represents a rate configuration for a rate limiter. This class encapsulates the maximum number of
 * permits (events) that can be processed per time unit, which is typically used in rate limiting
 * algorithms such as the token algorithm.
 */
public class Rate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The maximum number of permits (events) that can be processed per time unit. This value is
     * used to configure the rate limiter.
     */
    private long maxPerCount;

    /**
     * Constructs a Rate object with the specified maximum number of permits per time unit.
     *
     * @param maxPerCount the maximum number of permits (events) that can be processed per time
     *     unit.
     */
    public Rate(long maxPerCount) {
        this.maxPerCount = maxPerCount;
    }

    /**
     * Returns the maximum number of permits (events) that can be processed per time unit.
     *
     * @return the maximum number of permits per time unit.
     */
    public long getMaxPerCount() {
        return maxPerCount;
    }

    /**
     * Sets the maximum number of permits (events) that can be processed per time unit. This method
     * is useful for dynamically adjusting the rate limit.
     *
     * @param maxPerCount the new maximum number of permits per time unit.
     */
    public void setMaxPerCount(long maxPerCount) {
        this.maxPerCount = maxPerCount;
    }
}
