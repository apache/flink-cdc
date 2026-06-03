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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;

/** Tests for {@link MilvusRetryUtils}. */
class MilvusRetryUtilsTest {

    @Test
    void testClassifyErrors() {
        Assertions.assertThat(
                        MilvusRetryUtils.classify(
                                new IllegalArgumentException("Vector dimension mismatch")))
                .isEqualTo(MilvusConnectorErrorCode.VECTOR_DIMENSION_MISMATCH);
        Assertions.assertThat(
                        MilvusRetryUtils.classify(new RuntimeException("connection reset by peer")))
                .isEqualTo(MilvusConnectorErrorCode.TRANSIENT_MILVUS_ERROR);
        Assertions.assertThat(
                        MilvusRetryUtils.classify(
                                new IllegalStateException("Milvus collection name collision")))
                .isEqualTo(MilvusConnectorErrorCode.COLLECTION_COLLISION);
    }

    @Test
    void testRetryableMilvusErrors() {
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(
                                new MilvusClientException(ErrorCode.TIMEOUT, "timeout")))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(
                                new MilvusClientException(ErrorCode.RPC_ERROR, "rpc failed")))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(new RuntimeException("rate limit exceeded")))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(
                                new RuntimeException("connection reset by peer")))
                .isTrue();
    }

    @Test
    void testNonRetryableErrors() {
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(
                                new IllegalArgumentException("Vector dimension mismatch")))
                .isFalse();
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(
                                new MilvusClientException(
                                        ErrorCode.INVALID_PARAMS, "invalid params")))
                .isFalse();
        Assertions.assertThat(
                        MilvusRetryUtils.isRetryable(
                                new IllegalStateException(
                                        "collection does not exist and create-collection.enabled is false")))
                .isFalse();
    }

    @Test
    void testShouldReconnect() {
        Assertions.assertThat(
                        MilvusRetryUtils.shouldReconnect(
                                new MilvusClientException(ErrorCode.RPC_ERROR, "rpc")))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.shouldReconnect(new IOException(new ConnectException())))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.shouldReconnect(
                                new RuntimeException("rate limit exceeded")))
                .isFalse();
        Assertions.assertThat(
                        MilvusRetryUtils.shouldReconnect(
                                new IllegalArgumentException("dimension mismatch")))
                .isFalse();
    }

    @Test
    void testAlreadyExistsErrors() {
        Assertions.assertThat(
                        MilvusRetryUtils.isFieldAlreadyExistsError(
                                new RuntimeException("field already exists")))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.isIndexAlreadyExistsError(
                                new RuntimeException("duplicate index")))
                .isTrue();
        Assertions.assertThat(
                        MilvusRetryUtils.isPartitionAlreadyExistsError(
                                new RuntimeException("partition already exists")))
                .isTrue();
    }

    @Test
    void testExponentialBackoff() {
        Assertions.assertThat(MilvusRetryUtils.computeRetryBackoffMillis(200, 0)).isEqualTo(200);
        Assertions.assertThat(MilvusRetryUtils.computeRetryBackoffMillis(200, -1)).isEqualTo(200);
        Assertions.assertThat(MilvusRetryUtils.computeRetryBackoffMillis(200, 1)).isEqualTo(400);
        Assertions.assertThat(MilvusRetryUtils.computeRetryBackoffMillis(200, 10))
                .isEqualTo(30_000);
        Assertions.assertThat(MilvusRetryUtils.computeRetryBackoffMillis(Long.MAX_VALUE, 1))
                .isEqualTo(30_000);
    }
}
