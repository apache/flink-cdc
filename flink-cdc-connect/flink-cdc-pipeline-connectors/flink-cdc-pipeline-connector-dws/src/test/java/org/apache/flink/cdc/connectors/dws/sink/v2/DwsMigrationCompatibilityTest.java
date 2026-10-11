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

import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Compatibility gate for migrations from the removed staging/committer protocol. */
class DwsMigrationCompatibilityTest {

    @Test
    void rejectsLegacyWriterStateAndUnknownStateVersionWithMigrationDiagnostics() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        1, readHex("compatibility/legacy-writer-state-v1.hex")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("legacy staging writer state")
                .hasMessageContaining("native-client sink protocol");
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        99,
                                        serializer.serialize(DwsWriterState.nativeClientMarker())))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Unknown DWS writer state serializer version");
    }

    @Test
    void doesNotInterpretPendingLegacyCommittableAsNativeWriterState() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion(),
                                        readHex("compatibility/legacy-committable-v1.hex")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("marker");

        DwsSink sink =
                new DwsSink(
                        "jdbc:gaussdb://localhost:8000/test",
                        "user",
                        "password",
                        ZoneId.of("UTC"),
                        false,
                        "public",
                        true);
        assertThat(sink).isNotInstanceOf(TwoPhaseCommittingSink.class);
    }

    @Test
    void acceptsOnlyTheNewFixedMarker() throws Exception {
        DwsWriterStateSerializer serializer = new DwsWriterStateSerializer();

        assertThat(
                        serializer.deserialize(
                                serializer.getVersion(),
                                serializer.serialize(DwsWriterState.nativeClientMarker())))
                .isEqualTo(DwsWriterState.nativeClientMarker());
    }

    private static byte[] readHex(String resource) throws Exception {
        try (InputStream stream =
                DwsMigrationCompatibilityTest.class
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
}
