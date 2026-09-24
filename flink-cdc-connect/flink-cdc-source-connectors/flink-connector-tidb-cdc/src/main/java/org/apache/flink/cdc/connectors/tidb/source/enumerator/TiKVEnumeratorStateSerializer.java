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

package org.apache.flink.cdc.connectors.tidb.source.enumerator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplit;
import org.apache.flink.cdc.connectors.tidb.source.split.TiKVKeyRangeSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link TiKVEnumeratorState}. */
@Internal
public class TiKVEnumeratorStateSerializer
        implements SimpleVersionedSerializer<TiKVEnumeratorState> {

    public static final TiKVEnumeratorStateSerializer INSTANCE =
            new TiKVEnumeratorStateSerializer();

    private static final int VERSION = 1;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private final TiKVKeyRangeSplitSerializer splitSerializer =
            TiKVKeyRangeSplitSerializer.INSTANCE;

    private TiKVEnumeratorStateSerializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TiKVEnumeratorState state) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeBoolean(state.isEnumerated());
        out.writeInt(state.getParallelism());
        final List<TiKVKeyRangeSplit> splits = state.getUnassignedSplits();
        out.writeInt(splits.size());
        for (TiKVKeyRangeSplit split : splits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TiKVEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown TiKVEnumeratorState version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final boolean enumerated = in.readBoolean();
        final int parallelism = in.readInt();
        final int size = in.readInt();
        final List<TiKVKeyRangeSplit> splits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int length = in.readInt();
            byte[] splitBytes = new byte[length];
            in.readFully(splitBytes);
            splits.add(splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes));
        }
        return new TiKVEnumeratorState(splits, enumerated, parallelism);
    }
}
