/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;

/**
 * retry utils to execute a callable with specific retry times, set MAX_RETRIES and RETRY_DELAY at
 * the start of {@link org.apache.flink.cdc.connectors.maxcompute.sink.MaxComputeEventSink}.
 */
public class RetryUtils {

    public static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY = 1000;

    public static <T> T execute(Callable<T> callable) throws IOException {
        return execute(callable, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY);
    }

    public static <T> T execute(Callable<T> callable, int maxRetries, long retryDelay)
            throws IOException {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    throw new IOException("Failed after retries", e);
                }
                try {
                    LOG.warn("Retrying after exception: " + e.getMessage(), e);
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted", ie);
                }
            }
        }
    }

    public static <T> T executeUnchecked(Callable<T> callable) {
        return executeUnchecked(callable, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY);
    }

    public static <T> T executeUnchecked(Callable<T> callable, int maxRetries, long retryDelay) {
        try {
            return execute(callable, maxRetries, retryDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
