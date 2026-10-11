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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.assertj.core.api.Assertions.assertThat;

/** Freezes the legacy serializer bytes before the staging protocol is removed. */
class DwsLegacySerializerFixtureTest {

    @Test
    void shouldMatchLegacyWriterStateFixture() throws Exception {
        byte[] fixture = readHex("compatibility/legacy-writer-state-v1.hex");

        assertThat(fixture).isNotEmpty();
        assertThat(toHex(MessageDigest.getInstance("SHA-256").digest(fixture)))
                .isEqualTo("9c37fa4e9884d2a9b4cf8008efc34339c4474903d76d23bdeac2a644217a875b");
    }

    @Test
    void shouldMatchLegacyPendingCommittableFixture() throws Exception {
        byte[] fixture = readHex("compatibility/legacy-committable-v1.hex");

        assertThat(fixture).isNotEmpty();
        assertThat(toHex(MessageDigest.getInstance("SHA-256").digest(fixture)))
                .isEqualTo("a0be294ddeff2aa535c9307333fac7cd0af5536d6de8403fdcbe423861a872f6");
    }

    private static byte[] readHex(String resource) throws Exception {
        try (InputStream stream =
                DwsLegacySerializerFixtureTest.class
                        .getClassLoader()
                        .getResourceAsStream(resource)) {
            if (stream == null) {
                throw new IllegalStateException("Missing compatibility fixture: " + resource);
            }
            String hex =
                    new String(stream.readAllBytes(), StandardCharsets.US_ASCII)
                            .replaceAll("\\s", "");
            byte[] bytes = new byte[hex.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return bytes;
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            hex.append(String.format("%02x", value & 0xff));
        }
        return hex.toString();
    }
}
