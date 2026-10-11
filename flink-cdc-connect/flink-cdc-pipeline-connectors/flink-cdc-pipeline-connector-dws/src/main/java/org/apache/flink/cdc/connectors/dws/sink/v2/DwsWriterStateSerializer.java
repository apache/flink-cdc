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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Serializer for {@link DwsWriterState}. */
public class DwsWriterStateSerializer implements SimpleVersionedSerializer<DwsWriterState> {

    private static final int VERSION = 2;
    private static final byte[] MARKER = "DWS_NATIVE_CLIENT_V2".getBytes(StandardCharsets.US_ASCII);

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DwsWriterState state) throws IOException {
        if (!DwsWriterState.nativeClientMarker().equals(state)) {
            throw new IOException("Unsupported DWS writer state marker.");
        }
        return Arrays.copyOf(MARKER, MARKER.length);
    }

    @Override
    public DwsWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            throw new IOException(
                    "Cannot restore legacy staging writer state with the native-client sink protocol.");
        }
        if (version != VERSION) {
            throw new IOException("Unknown DWS writer state serializer version: " + version);
        }
        if (!Arrays.equals(MARKER, serialized)) {
            throw new IOException("Invalid DWS native-client writer state marker.");
        }
        return DwsWriterState.nativeClientMarker();
    }
}
