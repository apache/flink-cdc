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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.TableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** A serializer for the {@link PaimonWriterState}. */
public class PaimonWriterStateSerializer implements SimpleVersionedSerializer<PaimonWriterState> {

    private static final int currentVersion = 0;

    @Override
    public int getVersion() {
        return currentVersion;
    }

    @Override
    public byte[] serialize(PaimonWriterState paimonWriterState) throws IOException {
        try (ByteArrayOutputStream bao = new ByteArrayOutputStream(256)) {
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(bao);
            view.writeLong(paimonWriterState.getCheckpointId());
            return bao.toByteArray();
        }
    }

    @Override
    public PaimonWriterState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 0) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized)) {
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bis);
                long check = view.readLong();
                return new PaimonWriterState(check);
            }
        }
        throw new TableException(
                String.format(
                        "Can't serialized data with version %d because the max support version is %d.",
                        version, currentVersion));
    }
}
