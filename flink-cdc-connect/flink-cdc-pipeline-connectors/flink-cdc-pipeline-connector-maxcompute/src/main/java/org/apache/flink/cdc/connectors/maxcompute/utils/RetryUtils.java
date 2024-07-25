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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Retry utilities to execute a callable with specific retry times. Set MAX_RETRIES and RETRY_DELAY
 * at the start of {@link org.apache.flink.cdc.connectors.maxcompute.sink.MaxComputeEventSink}.
 */
public class RetryUtils {
    public static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_RETRY_DELAY = 5000;
    private static final RetryLogger RETRY_LOGGER = new RetryLogger();

    public static RetryLogger getRetryLogger() {
        return RETRY_LOGGER;
    }

    /**
     * Executes a callable with default retry strategy.
     *
     * @param callable the task to be executed
     * @param <T> the type of the task's result
     * @return the task result
     * @throws IOException If the task fails after all retries
     */
    public static <T> T execute(Callable<T> callable) throws IOException {
        return execute(callable, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY);
    }

    /**
     * Executes a callable with specific retry strategy.
     *
     * @param callable the task to be executed
     * @param maxRetries the maximum number of retries
     * @param retryDelay the delay between retries in milliseconds
     * @param <T> the type of the task's result
     * @return the task result
     * @throws IOException If the task fails after all retries
     */
    public static <T> T execute(Callable<T> callable, int maxRetries, long retryDelay)
            throws IOException {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (Exception e) {
                attempt++;
                if (attempt > maxRetries) {
                    if (e instanceof OdpsException) {
                        throw new IOException(
                                "Failed after retries. RequestId: "
                                        + ((OdpsException) e).getRequestId(),
                                e);
                    }
                    throw new IOException("Failed after retries", e);
                }
                try {
                    RETRY_LOGGER.onRetryLog(e, attempt, retryDelay);
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted", ie);
                }
            }
        }
    }

    /**
     * Executes a callable with default retry strategy and unchecked exceptions.
     *
     * @param callable the task to be executed
     * @param <T> the type of the task's result
     * @return the task result
     */
    public static <T> T executeUnchecked(Callable<T> callable) {
        return executeUnchecked(callable, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY);
    }

    /**
     * Executes a callable with specific retry strategy and unchecked exceptions.
     *
     * @param callable the task to be executed
     * @param maxRetries the maximum number of retries
     * @param retryDelay the delay between retries in milliseconds
     * @param <T> the type of the task's result
     * @return the task result
     */
    public static <T> T executeUnchecked(Callable<T> callable, int maxRetries, long retryDelay) {
        try {
            return execute(callable, maxRetries, retryDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static class RetryLogger extends RestClient.RetryLogger {
        @Override
        public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
            // Log the exception and retry details
            LOG.warn(
                    "Retry attempt #{} failed. Exception: {}. Sleeping for {} ms before next attempt.",
                    retryCount,
                    e.getMessage(),
                    retrySleepTime,
                    e);
        }
    }

    // retry 3 times and wait 5 seconds for each retry
    static class FlinkDefaultRetryPolicy implements TunnelRetryHandler.RetryPolicy {
        @Override
        public boolean shouldRetry(Exception e, int attempt) {
            return attempt <= DEFAULT_MAX_RETRIES;
        }

        @Override
        public long getRetryWaitTime(int attempt) {
            return TimeUnit.MILLISECONDS.toMillis(DEFAULT_RETRY_DELAY);
        }
    }
}
