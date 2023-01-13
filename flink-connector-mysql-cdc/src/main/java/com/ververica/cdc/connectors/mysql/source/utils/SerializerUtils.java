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

package com.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetKind;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffsetSerializer;
import io.debezium.DebeziumException;
import io.debezium.util.HexConverter;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Utils for serialization and deserialization. */
public class SerializerUtils {

    private SerializerUtils() {}

    public static void writeBinlogPosition(BinlogOffset offset, DataOutputSerializer out)
            throws IOException {
        out.writeBoolean(offset != null);
        if (offset != null) {
            byte[] binlogOffsetBytes = BinlogOffsetSerializer.INSTANCE.serialize(offset);
            out.writeInt(binlogOffsetBytes.length);
            out.write(binlogOffsetBytes);
        }
    }

    public static BinlogOffset readBinlogPosition(int offsetVersion, DataInputDeserializer in)
            throws IOException {
        switch (offsetVersion) {
            case 1:
                return in.readBoolean()
                        ? BinlogOffset.ofBinlogFilePosition(in.readUTF(), in.readLong())
                        : null;
            case 2:
            case 3:
            case 4:
                return readBinlogPosition(in);
            default:
                throw new IOException("Unknown version: " + offsetVersion);
        }
    }

    public static BinlogOffset readBinlogPosition(DataInputDeserializer in) throws IOException {
        boolean offsetNonNull = in.readBoolean();
        if (offsetNonNull) {
            int binlogOffsetBytesLength = in.readInt();
            byte[] binlogOffsetBytes = new byte[binlogOffsetBytesLength];
            in.readFully(binlogOffsetBytes);
            BinlogOffset offset = BinlogOffsetSerializer.INSTANCE.deserialize(binlogOffsetBytes);
            // Old version of binlog offset without offset kind
            if (offset.getOffsetKind() == null) {
                if (StringUtils.isEmpty(offset.getFilename())
                        && offset.getPosition() == Long.MIN_VALUE) {
                    return BinlogOffset.ofNonStopping();
                }
                if (StringUtils.isEmpty(offset.getFilename()) && offset.getPosition() == 0L) {
                    return BinlogOffset.ofEarliest();
                }
                // For other cases we treat it as a specific offset
                return BinlogOffset.builder()
                        .setOffsetKind(BinlogOffsetKind.SPECIFIC)
                        .setOffsetMap(offset.getOffset())
                        .build();
            }
            return offset;
        } else {
            return null;
        }
    }

    public static String rowToSerializedString(Object[] splitBoundary) {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(splitBoundary);
            return HexConverter.convertToHexString(bos.toByteArray());
        } catch (IOException e) {
            throw new DebeziumException(
                    String.format("Cannot serialize split boundary information %s", splitBoundary));
        }
    }

    public static Object[] serializedStringToRow(String serialized) {
        try (final ByteArrayInputStream bis =
                        new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (Object[]) ois.readObject();
        } catch (Exception e) {
            throw new DebeziumException(
                    String.format(
                            "Failed to deserialize split boundary with value '%s'", serialized),
                    e);
        }
    }
}
