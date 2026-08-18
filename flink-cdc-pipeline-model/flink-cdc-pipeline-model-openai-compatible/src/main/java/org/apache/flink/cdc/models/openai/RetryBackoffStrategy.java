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

package org.apache.flink.cdc.models.openai;

/** Strategy for calculating the delay between retry attempts. */
public enum RetryBackoffStrategy {
    FIXED {
        @Override
        long nextInterval(long currentIntervalMillis) {
            return currentIntervalMillis;
        }

        @Override
        long minimumTotalDelay(long baseIntervalMillis, int attempts) {
            return Math.multiplyExact(baseIntervalMillis, Math.max(0, attempts - 1));
        }
    },

    EXPONENTIAL {
        @Override
        long nextInterval(long currentIntervalMillis) {
            return Math.multiplyExact(currentIntervalMillis, 2L);
        }

        @Override
        long minimumTotalDelay(long baseIntervalMillis, int attempts) {
            int delays = Math.max(0, attempts - 1);
            if (delays >= Long.SIZE - 1) {
                throw new ArithmeticException("Too many exponential retry attempts");
            }
            long multiplier = Math.subtractExact(1L << delays, 1L);
            return Math.multiplyExact(baseIntervalMillis, multiplier);
        }
    };

    abstract long nextInterval(long currentIntervalMillis);

    abstract long minimumTotalDelay(long baseIntervalMillis, int attempts);
}
