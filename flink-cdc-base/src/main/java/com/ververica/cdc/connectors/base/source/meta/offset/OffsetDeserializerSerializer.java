/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.source.meta.offset;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static com.ververica.cdc.connectors.base.utils.SerializerUtils.serializedStringToObject;
import static com.ververica.cdc.connectors.base.utils.SerializerUtils.serializedStringToRow;

/** read {@link Offset} from input stream and write {@link Offset} to output stream. */
public interface OffsetDeserializerSerializer extends Serializable {

    OffsetFactory getOffsetFactory();

    default Offset readOffsetPosition(int offsetVersion, DataInputDeserializer in)
            throws IOException {
        switch (offsetVersion) {
            case 1:
                OffsetFactory offsetFactory = getOffsetFactory();
                return in.readBoolean()
                        ? offsetFactory.newOffset(in.readUTF(), in.readLong())
                        : null;
            case 2:
            case 3:
                return readOffsetPosition(in);
            default:
                throw new IOException("Unknown version: " + offsetVersion);
        }
    }

    default Offset readOffsetPosition(DataInputDeserializer in) throws IOException {
        boolean offsetNonNull = in.readBoolean();
        if (offsetNonNull) {
            int offsetBytesLength = in.readInt();
            byte[] offsetBytes = new byte[offsetBytesLength];
            in.readFully(offsetBytes);
            OffsetDeserializer offsetDeserializer = createOffsetDeserializer();
            return offsetDeserializer.deserialize(offsetBytes);
        } else {
            return null;
        }
    }

    default void writeOffsetPosition(Offset offset, DataOutputSerializer out) throws IOException {
        out.writeBoolean(offset != null);
        if (offset != null) {
            byte[] offsetBytes = OffsetSerializer.INSTANCE.serialize(offset);
            out.writeInt(offsetBytes.length);
            out.write(offsetBytes);
        }
    }

    default OffsetDeserializer createOffsetDeserializer() {
        return new OffsetDeserializer(getOffsetFactory());
    }

    default FinishedSnapshotSplitInfo deserialize(byte[] serialized) {
        try {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            TableId tableId = TableId.parse(in.readUTF());
            String splitId = in.readUTF();
            Object[] splitStart = serializedStringToRow(in.readUTF());
            Object[] splitEnd = serializedStringToRow(in.readUTF());
            OffsetFactory offsetFactory = (OffsetFactory) serializedStringToObject(in.readUTF());
            Offset highWatermark = readOffsetPosition(in);
            in.releaseArrays();

            return new FinishedSnapshotSplitInfo(
                    tableId, splitId, splitStart, splitEnd, highWatermark, offsetFactory);
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    /** Serializer for {@link Offset}. */
    class OffsetSerializer {

        public static final OffsetSerializer INSTANCE = new OffsetSerializer();

        public byte[] serialize(Offset offset) throws IOException {
            // use JSON serialization
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsBytes(offset.getOffset());
        }
    }

    /** Deserializer for {@link Offset}. */
    class OffsetDeserializer {

        private final OffsetFactory factory;

        public OffsetDeserializer(OffsetFactory offsetFactory) {
            this.factory = offsetFactory;
        }

        public Offset deserialize(byte[] bytes) throws IOException {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> offset = objectMapper.readValue(bytes, Map.class);

            return factory.newOffset(offset);
        }
    }
}
