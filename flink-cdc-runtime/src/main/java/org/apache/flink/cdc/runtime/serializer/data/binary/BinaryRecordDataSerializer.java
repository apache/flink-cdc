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

package org.apache.flink.cdc.runtime.serializer.data.binary;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinarySegmentUtils;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentWritable;

import java.io.IOException;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** Serializer for {@link BinaryRecordData}. */
@Internal
public class BinaryRecordDataSerializer extends TypeSerializerSingleton<BinaryRecordData> {

    private static final long serialVersionUID = 1L;

    public static final BinaryRecordDataSerializer INSTANCE = new BinaryRecordDataSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public BinaryRecordData createInstance() {
        return new BinaryRecordData(1);
    }

    @Override
    public BinaryRecordData copy(BinaryRecordData from) {
        return copy(from, new BinaryRecordData(from.getArity()));
    }

    @Override
    public BinaryRecordData copy(BinaryRecordData from, BinaryRecordData reuse) {
        return from.copy(reuse);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BinaryRecordData record, DataOutputView target) throws IOException {
        target.writeInt(record.getArity());
        target.writeInt(record.getSizeInBytes());
        if (target instanceof MemorySegmentWritable) {
            serializeWithoutLength(record, (MemorySegmentWritable) target);
        } else {
            BinarySegmentUtils.copyToView(
                    record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
        }
    }

    @Override
    public BinaryRecordData deserialize(DataInputView source) throws IOException {
        BinaryRecordData row = new BinaryRecordData(source.readInt());
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        row.pointTo(MemorySegmentFactory.wrap(bytes), 0, length);
        return row;
    }

    @Override
    @SuppressWarnings("unused")
    public BinaryRecordData deserialize(BinaryRecordData reuse, DataInputView source)
            throws IOException {
        MemorySegment[] segments = reuse.getSegments();
        checkArgument(
                segments == null || (segments.length == 1 && reuse.getOffset() == 0),
                "Reuse BinaryRecordData should have no segments or only one segment and offset start at 0.");
        // Note: arity is not used in BinaryRecordData, so we can ignore it here.But we still need
        // to read it.
        int arity = source.readInt();
        int length = source.readInt();
        if (segments == null || segments[0].size() < length) {
            segments = new MemorySegment[] {MemorySegmentFactory.wrap(new byte[length])};
        }
        source.readFully(segments[0].getArray(), 0, length);
        reuse.pointTo(segments, 0, length);
        return reuse;
    }

    // ============================ Page related operations ===================================

    private static void serializeWithoutLength(
            BinaryRecordData record, MemorySegmentWritable writable) throws IOException {
        if (record.getSegments().length == 1) {
            writable.write(record.getSegments()[0], record.getOffset(), record.getSizeInBytes());
        } else {
            serializeWithoutLengthSlow(record, writable);
        }
    }

    public static void serializeWithoutLengthSlow(
            BinaryRecordData record, MemorySegmentWritable out) throws IOException {
        int remainSize = record.getSizeInBytes();
        int posInSegOfRecord = record.getOffset();
        int segmentSize = record.getSegments()[0].size();
        for (MemorySegment segOfRecord : record.getSegments()) {
            int nWrite = Math.min(segmentSize - posInSegOfRecord, remainSize);
            assert nWrite > 0;
            out.write(segOfRecord, posInSegOfRecord, nWrite);

            // next new segment.
            posInSegOfRecord = 0;
            remainSize -= nWrite;
            if (remainSize == 0) {
                break;
            }
        }
        checkArgument(remainSize == 0);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<BinaryRecordData> snapshotConfiguration() {
        return new BinaryRecordDataSerializerSnapshot();
    }

    /** {@link TypeSerializerSnapshot} for {@link BinaryRecordDataSerializer}. */
    public static final class BinaryRecordDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<BinaryRecordData> {

        public BinaryRecordDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
