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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** A {@link SimpleVersionedSerializer} for {@link PaimonWriterState}. */
public class PaimonWriterStateSerializer implements SimpleVersionedSerializer<PaimonWriterState> {

    public static final PaimonWriterStateSerializer INSTANCE = new PaimonWriterStateSerializer();

    @Override
    public int getVersion() {
        return PaimonWriterState.VERSION;
    }

    @Override
    public byte[] serialize(PaimonWriterState paimonWriterState) throws IOException {
        if (paimonWriterState.getSerializedBytesCache() != null) {
            return paimonWriterState.getSerializedBytesCache();
        } else {
            final DataOutputSerializer out = new DataOutputSerializer(64);
            out.writeUTF(paimonWriterState.getCommitUser());
            byte[] serializedBytesCache = out.getCopyOfBuffer();
            paimonWriterState.setSerializedBytesCache(serializedBytesCache);
            return serializedBytesCache;
        }
    }

    @Override
    public PaimonWriterState deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        if (version == 0) {
            String commitUser = in.readUTF();
            return new PaimonWriterState(commitUser);
        }
        throw new IOException("Unknown version: " + version);
    }
}
