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

import java.io.Serializable;

/** An interface rate limiter. */
public interface RateLimiter extends Serializable {

    /**
     * A method that can be used to reset the rate limiter.
     *
     * @param numParallelism
     */
    void resetRate(int numParallelism);

    /**
     * Acquire method is a blocking call that is intended to be used in places where it is required
     * to limit the rate at which results are produced or other functions are called.
     *
     * @return The number of milliseconds this call blocked its caller.
     * @throws InterruptedException The interrupted exception.
     */
    int acquire() throws InterruptedException;
}
