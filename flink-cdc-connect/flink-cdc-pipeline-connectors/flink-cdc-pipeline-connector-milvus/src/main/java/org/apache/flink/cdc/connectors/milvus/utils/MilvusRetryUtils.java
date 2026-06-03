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

package org.apache.flink.cdc.connectors.milvus.utils;

import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;

import java.io.EOFException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

/** Retry classification helpers for Milvus sink operations. */
public class MilvusRetryUtils {

    private static final String RATE_LIMIT_EXCEEDED = "rate limit exceeded";
    private static final String MESSAGE_TOO_LARGE = "received message larger than max";

    private MilvusRetryUtils() {}

    public static MilvusConnectorErrorCode classify(Throwable throwable) {
        if (throwable == null) {
            return MilvusConnectorErrorCode.NON_RETRYABLE_MILVUS_ERROR;
        }
        if (containsMessageFragment(throwable, "collection name collision")) {
            return MilvusConnectorErrorCode.COLLECTION_COLLISION;
        }
        if (containsMessageFragment(throwable, "dimension mismatch")) {
            return MilvusConnectorErrorCode.VECTOR_DIMENSION_MISMATCH;
        }
        if (containsMessageFragment(throwable, "exceeds max length")) {
            return MilvusConnectorErrorCode.VARCHAR_LENGTH_EXCEEDED;
        }
        if (containsMessageFragment(throwable, "schema mismatch")
                || containsMessageFragment(throwable, "field not found")) {
            return MilvusConnectorErrorCode.SCHEMA_MISMATCH;
        }
        if (containsMessageFragment(throwable, "create-collection.enabled is false")) {
            return MilvusConnectorErrorCode.CONFIGURATION_ERROR;
        }
        if (isRetryable(throwable)) {
            return MilvusConnectorErrorCode.TRANSIENT_MILVUS_ERROR;
        }
        return MilvusConnectorErrorCode.NON_RETRYABLE_MILVUS_ERROR;
    }

    public static boolean isRetryable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (isNonRetryableExceptionType(throwable)) {
            return false;
        }
        if (containsNonRetryableMessage(throwable)) {
            return false;
        }
        if (throwable instanceof MilvusClientException) {
            return isRetryableMilvusError((MilvusClientException) throwable);
        }
        if (isTransientConnectionFailure(throwable)) {
            return true;
        }
        return containsRetryableMessage(throwable);
    }

    public static boolean shouldReconnect(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof MilvusClientException) {
            ErrorCode errorCode = ((MilvusClientException) throwable).getErrorCode();
            return errorCode == ErrorCode.RPC_ERROR
                    || errorCode == ErrorCode.TIMEOUT
                    || errorCode == ErrorCode.SERVER_ERROR;
        }
        return isTransientConnectionFailure(throwable)
                || containsTransientConnectionMessage(throwable);
    }

    public static boolean isPartitionAlreadyExistsError(Throwable throwable) {
        return containsMessageFragment(throwable, "partition already exist");
    }

    public static boolean isFieldAlreadyExistsError(Throwable throwable) {
        return containsMessageFragment(throwable, "field already exist")
                || containsMessageFragment(throwable, "duplicate field");
    }

    public static boolean isIndexAlreadyExistsError(Throwable throwable) {
        return containsMessageFragment(throwable, "index already exist")
                || containsMessageFragment(throwable, "duplicate index");
    }

    public static boolean isAdaptiveSplitError(Throwable throwable) {
        return containsMessageFragment(throwable, RATE_LIMIT_EXCEEDED)
                || containsMessageFragment(throwable, MESSAGE_TOO_LARGE);
    }

    public static long computeRetryBackoffMillis(long baseBackoffMillis, int attempt) {
        if (baseBackoffMillis <= 0) {
            return 0;
        }
        if (baseBackoffMillis >= 30_000L) {
            return 30_000L;
        }
        int cappedAttempt = Math.max(0, Math.min(attempt, 10));
        long multiplier = 1L << cappedAttempt;
        if (baseBackoffMillis > Long.MAX_VALUE / multiplier) {
            return 30_000L;
        }
        long backoff = baseBackoffMillis * multiplier;
        return Math.min(backoff, 30_000L);
    }

    private static boolean isRetryableMilvusError(MilvusClientException exception) {
        switch (exception.getErrorCode()) {
            case SERVER_ERROR:
            case RPC_ERROR:
            case TIMEOUT:
                return true;
            case INVALID_PARAMS:
            case COLLECTION_NOT_FOUND:
            case CLIENT_ERROR:
            default:
                return isAdaptiveSplitError(exception);
        }
    }

    private static boolean isNonRetryableExceptionType(Throwable throwable) {
        return throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException
                || throwable instanceof UnsupportedOperationException;
    }

    private static boolean isTransientConnectionFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof ConnectException
                    || current instanceof SocketTimeoutException
                    || current instanceof TimeoutException
                    || current instanceof UnknownHostException
                    || current instanceof NoRouteToHostException
                    || current instanceof InterruptedIOException
                    || current instanceof EOFException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean containsNonRetryableMessage(Throwable throwable) {
        return containsMessageFragment(throwable, "dimension mismatch")
                || containsMessageFragment(throwable, "schema mismatch")
                || containsMessageFragment(throwable, "field not found")
                || containsMessageFragment(throwable, "collection name collision")
                || containsMessageFragment(throwable, "exceeds max length")
                || containsMessageFragment(throwable, "unsupported")
                || containsMessageFragment(throwable, "must not be null")
                || containsMessageFragment(
                        throwable, "does not exist and create-collection.enabled is false");
    }

    private static boolean containsRetryableMessage(Throwable throwable) {
        return isAdaptiveSplitError(throwable) || containsTransientConnectionMessage(throwable);
    }

    private static boolean containsTransientConnectionMessage(Throwable throwable) {
        return containsMessageFragment(throwable, "unavailable")
                || containsMessageFragment(throwable, "connection reset")
                || containsMessageFragment(throwable, "connection refused")
                || containsMessageFragment(throwable, "broken pipe")
                || containsMessageFragment(throwable, "deadline exceeded")
                || containsMessageFragment(throwable, "timed out");
    }

    private static boolean containsMessageFragment(Throwable throwable, String fragment) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null
                    && message.toLowerCase(Locale.ROOT)
                            .contains(fragment.toLowerCase(Locale.ROOT))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
