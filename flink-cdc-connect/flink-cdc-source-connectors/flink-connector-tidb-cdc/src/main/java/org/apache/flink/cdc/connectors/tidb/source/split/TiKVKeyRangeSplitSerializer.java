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

package org.apache.flink.cdc.connectors.tidb.source.split;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** Serializer for {@link TiKVKeyRangeSplit}. */
@Internal
public class TiKVKeyRangeSplitSerializer implements SimpleVersionedSerializer<TiKVKeyRangeSplit> {

    public static final TiKVKeyRangeSplitSerializer INSTANCE = new TiKVKeyRangeSplitSerializer();

    private static final int VERSION = 1;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private TiKVKeyRangeSplitSerializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TiKVKeyRangeSplit split) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeUTF(split.splitId());
        writeByteArray(out, split.getStartKey());
        writeByteArray(out, split.getEndKey());
        out.writeLong(split.getResolvedTs());
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TiKVKeyRangeSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown TiKVKeyRangeSplit version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final String splitId = in.readUTF();
        final byte[] startKey = readByteArray(in);
        final byte[] endKey = readByteArray(in);
        final long resolvedTs = in.readLong();
        return new TiKVKeyRangeSplit(splitId, startKey, endKey, resolvedTs);
    }

    private static void writeByteArray(DataOutputSerializer out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readByteArray(DataInputDeserializer in) throws IOException {
        final int length = in.readInt();
        final byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }
}
