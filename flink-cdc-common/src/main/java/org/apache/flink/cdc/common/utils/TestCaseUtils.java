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

package org.apache.flink.cdc.common.utils;

import org.apache.flink.util.function.SupplierWithException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Some utility methods for creating repeated-checking test cases. */
public class TestCaseUtils {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(1);

    /** Fetch with a ({@code timeout}, {@code interval}) duration. */
    public static void repeatedCheck(Supplier<Boolean> fetcher) {
        repeatedCheck(fetcher, DEFAULT_TIMEOUT);
    }

    /** Fetch with a ({@code timeout}, {@code interval}) duration. */
    public static void repeatedCheck(Supplier<Boolean> fetcher, Duration timeout) {
        repeatedCheck(fetcher, timeout, DEFAULT_INTERVAL);
    }

    /** Fetch with a ({@code timeout}, {@code interval}) duration. */
    public static void repeatedCheck(
            Supplier<Boolean> fetcher, Duration timeout, Duration interval) {
        repeatedCheck(fetcher::get, timeout, interval, Collections.emptyList());
    }

    /** Fetch and wait with a ({@code timeout}, {@code interval}) duration. */
    public static <T> void repeatedCheck(
            Supplier<T> fetcher, Predicate<T> validator, Duration timeout, Duration interval) {
        repeatedCheckAndValidate(
                fetcher::get, validator, timeout, interval, Collections.emptyList());
    }

    /** Waiting for fetching values with a ({@code timeout}, {@code interval}) duration. */
    public static void repeatedCheck(
            SupplierWithException<Boolean, Throwable> fetcher,
            Duration timeout,
            Duration interval,
            List<Class<? extends Throwable>> allowedThrowsList) {
        repeatedCheckAndValidate(fetcher, b -> b, timeout, interval, allowedThrowsList);
    }

    /** Fetch and validate, with a ({@code timeout}, {@code interval}) duration. */
    public static <T> void repeatedCheckAndValidate(
            SupplierWithException<T, Throwable> fetcher,
            Predicate<T> validator,
            Duration timeout,
            Duration interval,
            List<Class<? extends Throwable>> allowedThrowsList) {

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.toMillis()) {
            try {
                if (validator.test(fetcher.get())) {
                    return;
                }
            } catch (Throwable t) {
                if (allowedThrowsList.stream()
                        .noneMatch(clazz -> clazz.isAssignableFrom(t.getClass()))) {
                    throw new RuntimeException("Fetcher has thrown an unexpected exception: ", t);
                }
            }
            try {
                Thread.sleep(interval.toMillis());
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
        throw new RuntimeException("Timeout when waiting for state to be ready.");
    }
}
