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

package org.apache.flink.cdc.connectors.gaussdb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.gaussdb.source.offset.GaussDBOffset;

import io.debezium.connector.gaussdb.connection.Lsn;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GaussDBStreamFetchTask}. */
class GaussDBStreamFetchTaskTest {

    @Test
    void testTaskCreation() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        assertThat(task).isNotNull();
        assertThat(task.getSplit()).isEqualTo(streamSplit);
        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testIsRunning() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testGetSplit() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        assertThat(task.getSplit()).isEqualTo(streamSplit);
        assertThat(task.getSplit().splitId()).isEqualTo("stream-split-test");
    }

    @Test
    void testClose() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        task.close();

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testCommitCurrentOffsetWithNullOffset() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        // Should not throw exception when offset is null
        task.commitCurrentOffset(null);

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testCommitCurrentOffsetWithValidOffset() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        GaussDBOffset offset = new GaussDBOffset(Lsn.valueOf(100L));

        // Should not throw exception when replication stream is not initialized
        // (it will just log a debug message)
        task.commitCurrentOffset(offset);

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testCommitCurrentOffsetWithInvalidLsn() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        GaussDBOffset offset = new GaussDBOffset(Lsn.INVALID_LSN);

        // Should not throw exception with invalid LSN
        task.commitCurrentOffset(offset);

        assertThat(task.isRunning()).isFalse();
    }

    @Test
    void testMultipleCloseCallsAreSafe() {
        StreamSplit streamSplit = createTestStreamSplit();
        GaussDBStreamFetchTask task = new GaussDBStreamFetchTask(streamSplit);

        task.close();
        task.close();
        task.close();

        assertThat(task.isRunning()).isFalse();
    }

    private StreamSplit createTestStreamSplit() {
        GaussDBOffset startOffset = new GaussDBOffset(Lsn.valueOf(0L));
        GaussDBOffset endOffset = GaussDBOffset.NO_STOPPING_OFFSET;

        return new StreamSplit("stream-split-test", startOffset, endOffset, null, null, 0);
    }
}
