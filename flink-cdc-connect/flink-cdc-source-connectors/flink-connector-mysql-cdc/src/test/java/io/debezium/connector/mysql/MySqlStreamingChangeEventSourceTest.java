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

package io.debezium.connector.mysql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLContext;

import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;

import static io.debezium.connector.mysql.MySqlStreamingChangeEventSource.resolveBinlogSslProtocol;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MySqlStreamingChangeEventSource}. */
class MySqlStreamingChangeEventSourceTest {

    @ParameterizedTest(name = "configured={0}, detected={1}, expected={2}")
    @MethodSource("protocolResolutionProvider")
    void testResolveBinlogSslProtocol(
            String configuredProtocol, String detectedProtocol, String expectedProtocol) {
        String result = resolveBinlogSslProtocol(configuredProtocol, detectedProtocol);
        assertThat(result).isEqualTo(expectedProtocol);
    }

    private static Stream<Arguments> protocolResolutionProvider() {
        return Stream.of(
                // When configured protocol is set, it should be used
                Arguments.of("TLSv1.2", "TLSv1.3", "TLSv1.2"),
                Arguments.of("TLSv1.3", "TLSv1.2", "TLSv1.3"),
                Arguments.of("TLSv1.1", "TLSv1.3", "TLSv1.1"),
                // When configured protocol is empty, detected should be used
                Arguments.of("", "TLSv1.3", "TLSv1.3"),
                Arguments.of("", "TLSv1.2", "TLSv1.2"),
                // When configured protocol is null, detected should be used
                Arguments.of(null, "TLSv1.3", "TLSv1.3"),
                Arguments.of(null, "TLSv1.2", "TLSv1.2"));
    }

    @ParameterizedTest(name = "protocol={0}")
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void testValidProtocolsAreAcceptedBySSLContext(String protocol)
            throws NoSuchAlgorithmException {
        // Verify that valid protocols can be used to create an SSLContext
        SSLContext context = SSLContext.getInstance(protocol);
        assertThat(context).isNotNull();
        assertThat(context.getProtocol()).isEqualTo(protocol);
    }

    @ParameterizedTest(name = "invalidProtocol={0}")
    @ValueSource(strings = {"InvalidProtocol", "TLSv99", "SSLv2"})
    void testInvalidProtocolsAreRejectedBySSLContext(String invalidProtocol) {
        // Verify that invalid protocols cause SSLContext.getInstance() to fail
        assertThatThrownBy(() -> SSLContext.getInstance(invalidProtocol))
                .isInstanceOf(NoSuchAlgorithmException.class);
    }

    @Test
    void testBinlogSslProtocolConstant() {
        // Verify the constant value matches what users would configure
        assertThat(MySqlStreamingChangeEventSource.BINLOG_SSL_PROTOCOL)
                .isEqualTo("binlog.ssl.protocol");
    }
}
