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

package com.ververica.cdc.connectors.oracle.source.utils;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffsetSerializer;
import io.debezium.DebeziumException;
import io.debezium.util.HexConverter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Utils for serialization and deserialization. */
public class SerializerUtils {

    private SerializerUtils() {}

    public static void writeRedoLogPosition(RedoLogOffset offset, DataOutputSerializer out)
            throws IOException {
        out.writeBoolean(offset != null);
        if (offset != null) {
            byte[] redoLogOffsetBytes = RedoLogOffsetSerializer.INSTANCE.serialize(offset);
            out.writeInt(redoLogOffsetBytes.length);
            out.write(redoLogOffsetBytes);
        }
    }

    public static RedoLogOffset readRedoLogPosition(int offsetVersion, DataInputDeserializer in)
            throws IOException {
        switch (offsetVersion) {
            case 1:
                return in.readBoolean() ? new RedoLogOffset(in.readUTF(), in.readLong()) : null;
            case 2:
            case 3:
            case 4:
                return readRedoLogPosition(in);
            default:
                throw new IOException("Unknown version: " + offsetVersion);
        }
    }

    public static RedoLogOffset readRedoLogPosition(DataInputDeserializer in) throws IOException {
        boolean offsetNonNull = in.readBoolean();
        if (offsetNonNull) {
            int redoLogOffsetBytesLength = in.readInt();
            byte[] redoLogOffsetBytes = new byte[redoLogOffsetBytesLength];
            in.readFully(redoLogOffsetBytes);
            return RedoLogOffsetSerializer.INSTANCE.deserialize(redoLogOffsetBytes);
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

    public static String rowToSerializedString(Object splitBoundary) {
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

    public static Object serializedStringToObject(String serialized) {
        try (final ByteArrayInputStream bis =
                        new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (Exception e) {
            throw new DebeziumException(
                    String.format(
                            "Failed to deserialize split boundary with value '%s'", serialized),
                    e);
        }
    }
}
