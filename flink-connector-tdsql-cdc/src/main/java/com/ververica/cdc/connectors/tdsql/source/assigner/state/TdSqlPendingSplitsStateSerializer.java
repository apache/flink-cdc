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

package com.ververica.cdc.connectors.tdsql.source.assigner.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsStateSerializer;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplitSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the {@link
 * com.ververica.cdc.connectors.tdsql.source.split.TdSqlSplitState} of TdSQL CDC source.
 */
public class TdSqlPendingSplitsStateSerializer
        implements SimpleVersionedSerializer<TdSqlPendingSplitsState> {

    private final PendingSplitsStateSerializer mySqlStateSerializer;
    private final TdSqlSplitSerializer splitSerializer;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    public TdSqlPendingSplitsStateSerializer(TdSqlSplitSerializer splitSerializer) {
        this.mySqlStateSerializer =
                new PendingSplitsStateSerializer(splitSerializer.mySqlSplitSerializer());
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return this.mySqlStateSerializer.getVersion();
    }

    @Override
    public byte[] serialize(TdSqlPendingSplitsState state) throws IOException {
        if (state.isSerialized()) {
            return state.getSerializedCache();
        }
        Map<TdSqlSet, PendingSplitsState> sets = state.getStateMap();

        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        for (TdSqlSet set : sets.keySet()) {
            splitSerializer.serializeTdSqlSet(set, out);

            PendingSplitsState pendingSplitsState = sets.get(set);
            final byte[] mySqlPendingSplitStateBytes =
                    mySqlStateSerializer.serialize(pendingSplitsState);

            out.writeInt(mySqlPendingSplitStateBytes.length);
            out.write(mySqlPendingSplitStateBytes);
        }

        final byte[] result = out.getCopyOfBuffer();
        state.setSerializedCache(result);

        return result;
    }

    @Override
    public TdSqlPendingSplitsState deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        Map<TdSqlSet, PendingSplitsState> stateMap = new HashMap<>();
        while (in.available() > 0) {
            TdSqlSet set = splitSerializer.deserializeTdSqlSet(in);

            int len = in.readInt();

            byte[] mySqlPendingSplitStateBytes = new byte[len];
            in.read(mySqlPendingSplitStateBytes);

            PendingSplitsState pendingSplitsState =
                    mySqlStateSerializer.deserialize(version, mySqlPendingSplitStateBytes);
            stateMap.put(set, pendingSplitsState);
        }

        return new TdSqlPendingSplitsState(stateMap);
    }
}
