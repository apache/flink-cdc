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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link DwsCommittable}. */
public class DwsCommittableSerializer implements SimpleVersionedSerializer<DwsCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DwsCommittable committable) throws IOException {
        java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
        out.writeUTF(committable.getJobId());
        out.writeLong(committable.getCheckpointId());
        out.writeInt(committable.getSubtaskId());
        out.writeUTF(committable.getTargetSchema());
        out.writeUTF(committable.getTargetTable());
        out.writeUTF(committable.getStagingSchema());
        out.writeUTF(committable.getStagingTable());
        writeStringList(out, committable.getColumnNames());
        writeStringList(out, committable.getPrimaryKeys());
        out.flush();
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public DwsCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown DWS committable serializer version: " + version);
        }

        DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(serialized));
        return new DwsCommittable(
                in.readUTF(),
                in.readLong(),
                in.readInt(),
                in.readUTF(),
                in.readUTF(),
                in.readUTF(),
                in.readUTF(),
                readStringList(in),
                readStringList(in));
    }

    private static void writeStringList(DataOutputStream out, List<String> values)
            throws IOException {
        out.writeInt(values.size());
        for (String value : values) {
            out.writeUTF(value);
        }
    }

    private static List<String> readStringList(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readUTF());
        }
        return values;
    }
}
