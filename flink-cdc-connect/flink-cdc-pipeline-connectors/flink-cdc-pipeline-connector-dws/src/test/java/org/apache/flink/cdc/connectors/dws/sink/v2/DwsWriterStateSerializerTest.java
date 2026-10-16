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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DwsWriterState} and {@link DwsWriterStateSerializer}. */
class DwsWriterStateSerializerTest {

    @Test
    void testWriterStateRoundTrip() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        DwsWriterState restored =
                serializer.deserialize(
                        serializer.getVersion(),
                        serializer.serialize(DwsWriterState.nativeClientMarker()));

        assertThat(serializer.getVersion()).isEqualTo(2);
        assertThat(restored).isEqualTo(DwsWriterState.nativeClientMarker());
        assertThat(serializer.serialize(restored))
                .isEqualTo("DWS_NATIVE_CLIENT_V2".getBytes(StandardCharsets.US_ASCII));
    }

    @Test
    void testRejectLegacyStagingStateAndUnknownVersion() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();
        byte[] serialized = serializer.serialize(DwsWriterState.nativeClientMarker());
        byte[] legacy = readHex("compatibility/legacy-writer-state-v1.hex");

        assertThatThrownBy(() -> serializer.deserialize(1, legacy))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("legacy staging writer state");

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, serialized))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Unknown DWS writer state serializer version");
    }

    @Test
    void testRejectCorruptMarker() {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(),
                                        "not-the-marker".getBytes(StandardCharsets.US_ASCII)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("marker");
    }

    private static byte[] readHex(String resource) throws Exception {
        try (InputStream stream =
                DwsWriterStateSerializerTest.class.getClassLoader().getResourceAsStream(resource)) {
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
}
