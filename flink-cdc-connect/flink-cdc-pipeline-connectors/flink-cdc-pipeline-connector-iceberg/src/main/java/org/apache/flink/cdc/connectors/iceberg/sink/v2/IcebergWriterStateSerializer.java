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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** A {@link IcebergWriterStateSerializer} for {@link IcebergWriterState}. */
public class IcebergWriterStateSerializer implements SimpleVersionedSerializer<IcebergWriterState> {

    private static final int VERSION = 0;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IcebergWriterState icebergWriterState) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeUTF(icebergWriterState.getJobId());
        out.writeUTF(icebergWriterState.getOperatorId());
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public IcebergWriterState deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        return new IcebergWriterState(in.readUTF(), in.readUTF());
    }
}
