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

package io.debezium.connector.oracle.xstream;

import org.apache.flink.cdc.connectors.oracle.source.reader.fetch.StoppableChangeEventSourceContext;

import oracle.streams.StreamsException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for XStream attach retry lifecycle helpers. */
class XstreamStreamingChangeEventSourceTest {

    @Test
    void testRetryWhenSessionBusyEventuallySucceeds() throws Exception {
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sleeps = new AtomicInteger();

        String result =
                XstreamStreamingChangeEventSource.retryWhenSessionBusy(
                        context,
                        "DBZXOUT",
                        () -> {
                            if (attempts.incrementAndGet() < 3) {
                                throw new StreamsException(
                                        "ORA-26812: An active session currently attached to XStream server \"DBZXOUT\"");
                            }
                            return "attached";
                        },
                        delayMillis -> sleeps.incrementAndGet(),
                        XstreamStreamingChangeEventSource.XSTREAM_ATTACH_MAX_RETRIES);

        assertThat(result).isEqualTo("attached");
        assertThat(attempts).hasValue(3);
        assertThat(sleeps).hasValue(2);
    }

    @Test
    void testRetryWhenSessionBusyStopsWhenContextClosed() throws Exception {
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sleeps = new AtomicInteger();

        String result =
                XstreamStreamingChangeEventSource.retryWhenSessionBusy(
                        context,
                        "DBZXOUT",
                        () -> {
                            attempts.incrementAndGet();
                            throw new StreamsException(
                                    "ORA-26812: An active session currently attached to XStream server \"DBZXOUT\"");
                        },
                        delayMillis -> {
                            sleeps.incrementAndGet();
                            context.stopChangeEventSource();
                        },
                        XstreamStreamingChangeEventSource.XSTREAM_ATTACH_MAX_RETRIES);

        assertThat(result).isNull();
        assertThat(attempts).hasValue(1);
        assertThat(sleeps).hasValue(1);
    }

    @Test
    void testRetryWhenSessionBusyDoesNotRetryOtherErrors() {
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(
                        () ->
                                XstreamStreamingChangeEventSource.retryWhenSessionBusy(
                                        context,
                                        "DBZXOUT",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new StreamsException(
                                                    "ORA-00001: unique constraint");
                                        },
                                        delayMillis -> {},
                                        XstreamStreamingChangeEventSource
                                                .XSTREAM_ATTACH_MAX_RETRIES))
                .isInstanceOf(StreamsException.class)
                .hasMessageContaining("ORA-00001");
        assertThat(attempts).hasValue(1);
    }

    @Test
    void testRetryWhenSessionBusyExhaustedThrowsClearException() {
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sleeps = new AtomicInteger();

        assertThatThrownBy(
                        () ->
                                XstreamStreamingChangeEventSource.retryWhenSessionBusy(
                                        context,
                                        "DBZXOUT",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new StreamsException(
                                                    "ORA-26812: An active session currently attached to XStream server \"DBZXOUT\"");
                                        },
                                        delayMillis -> sleeps.incrementAndGet(),
                                        XstreamStreamingChangeEventSource
                                                .XSTREAM_ATTACH_MAX_RETRIES))
                .isInstanceOf(StreamsException.class)
                .hasMessageContaining("Please clean the residual session and retry");
        assertThat(attempts)
                .hasValue(XstreamStreamingChangeEventSource.XSTREAM_ATTACH_MAX_RETRIES + 1);
        assertThat(sleeps).hasValue(XstreamStreamingChangeEventSource.XSTREAM_ATTACH_MAX_RETRIES);
    }

    @Test
    void testRetryWhenSessionBusyDisabledThrowsImmediately() {
        StoppableChangeEventSourceContext context = new StoppableChangeEventSourceContext();
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger sleeps = new AtomicInteger();

        assertThatThrownBy(
                        () ->
                                XstreamStreamingChangeEventSource.retryWhenSessionBusy(
                                        context,
                                        "DBZXOUT",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new StreamsException(
                                                    "ORA-26812: An active session currently attached to XStream server \"DBZXOUT\"");
                                        },
                                        delayMillis -> sleeps.incrementAndGet(),
                                        0))
                .isInstanceOf(StreamsException.class)
                .hasMessageContaining("Please clean the residual session and retry");
        assertThat(attempts).hasValue(1);
        assertThat(sleeps).hasValue(0);
    }
}
