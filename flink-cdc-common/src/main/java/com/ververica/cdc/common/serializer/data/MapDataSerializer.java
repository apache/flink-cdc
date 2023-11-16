/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.common.serializer.data;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.data.MapData;

import java.io.IOException;

/** TODO : Serializer for {@link MapData}. */
public class MapDataSerializer extends TypeSerializer<MapData> {

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<MapData> duplicate() {
        return null;
    }

    @Override
    public MapData createInstance() {
        return null;
    }

    @Override
    public MapData copy(MapData mapData) {
        return null;
    }

    @Override
    public MapData copy(MapData mapData, MapData t1) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(MapData mapData, DataOutputView dataOutputView) throws IOException {}

    @Override
    public MapData deserialize(DataInputView dataInputView) throws IOException {
        return null;
    }

    @Override
    public MapData deserialize(MapData mapData, DataInputView dataInputView) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView)
            throws IOException {}

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<MapData> snapshotConfiguration() {
        return null;
    }
}
