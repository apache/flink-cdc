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

package org.apache.flink.cdc.common.testutils;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.function.SupplierWithException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

    public static <T> List<T> fetch(Iterator<T> iter, final int size) throws InterruptedException {
        return fetch(iter, size, DEFAULT_TIMEOUT);
    }

    public static <T> List<T> fetch(Iterator<T> iter, final int size, Duration timeout)
            throws InterruptedException {
        return fetch(iter, size, timeout, DEFAULT_INTERVAL);
    }

    /**
     * Fetches at most {@code size} entries from {@link Iterator} {@code iter}. <br>
     * It may return a list with less than {@code size} elements, if {@code iter} doesn't provide
     * results or {@code timeout} exceeds.
     */
    public static <T> List<T> fetch(
            Iterator<T> iter, final int size, Duration timeout, Duration interval)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        ConcurrentLinkedQueue<T> results = new ConcurrentLinkedQueue<>();
        AtomicReference<Throwable> fetchException = new AtomicReference<>();

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                int remainingSize = size;
                                while (remainingSize > 0 && iter.hasNext()) {
                                    T row = iter.next();
                                    results.add(row);
                                    remainingSize--;
                                }
                            } catch (Throwable t) {
                                fetchException.set(t);
                            }
                        });

        thread.start();

        while (true) {
            // Raise any exception thrown by the fetching thread
            if (fetchException.get() != null) {
                throw (RuntimeException) fetchException.get();
            }

            // Stop if fetching thread has exited
            if (!thread.isAlive()) {
                break;
            }

            // Stop waiting if deadline has arrived
            if (System.currentTimeMillis() > deadline) {
                thread.interrupt();
                break;
            }

            Thread.sleep(interval.toMillis());
        }

        return new ArrayList<>(results);
    }

    public static <S, T> List<T> fetchAndConvert(
            Iterator<S> iter, int size, Function<S, T> converter) throws InterruptedException {
        return fetch(iter, size).stream().map(converter).collect(Collectors.toList());
    }

    public static <S, T> List<T> fetchAndConvert(
            Iterator<S> iter, int size, Duration timeout, Function<S, T> converter)
            throws InterruptedException {
        return fetch(iter, size, timeout).stream().map(converter).collect(Collectors.toList());
    }

    public static void waitForSnapshotStarted(Iterator<?> iter) {
        repeatedCheck(iter::hasNext);
    }

    public static void waitForSnapshotStarted(String sinkName) throws InterruptedException {
        waitForSinkSize(sinkName, false, 1);
    }

    public static void waitForSinkSize(String sinkName, boolean upsertMode, int expectedSize)
            throws InterruptedException {
        waitForSinkSize(sinkName, upsertMode, DEFAULT_TIMEOUT, expectedSize);
    }

    public static void waitForSinkSize(
            String sinkName, boolean upsertMode, Duration timeout, int expectedSize)
            throws InterruptedException {
        waitForSinkSize(sinkName, upsertMode, expectedSize, timeout, DEFAULT_INTERVAL);
    }

    public static void waitForSinkSize(
            String sinkName,
            boolean upsertMode,
            int expectedSize,
            Duration timeout,
            Duration interval)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (sinkSize(sinkName, upsertMode) < expectedSize) {
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException(
                        String.format(
                                "Wait for sink size timeout. Expected %s, got actual %s",
                                expectedSize, sinkSize(sinkName, upsertMode)));
            }
            Thread.sleep(interval.toMillis());
        }
    }

    public static int sinkSize(String sinkName, boolean upsertMode) {
        synchronized (TestValuesTableFactory.class) {
            try {
                if (upsertMode) {
                    return TestValuesTableFactory.getResultsAsStrings(sinkName).size();
                } else {
                    return TestValuesTableFactory.getRawResultsAsStrings(sinkName).size();
                }
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }
}
