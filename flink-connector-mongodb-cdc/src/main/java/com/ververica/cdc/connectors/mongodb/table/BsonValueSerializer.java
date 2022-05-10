/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.io.IOException;

/** Serializer for {@link BsonValue} to support schema-less Bson values. */
public class BsonValueSerializer extends TypeSerializer<BsonValue> {

    private static final long serialVersionUID = 1L;

    private static final String WRAPPED_KEY = "_";

    public static final BsonValueSerializer INSTANCE = new BsonValueSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<BsonValue> duplicate() {
        return new BsonValueSerializer();
    }

    @Override
    public BsonValue createInstance() {
        return new BsonNull();
    }

    @Override
    public BsonValue copy(BsonValue from) {
        if (from == null) {
            return null;
        }
        return deserialize(serialize(from));
    }

    @Override
    public BsonValue copy(BsonValue from, BsonValue reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BsonValue record, DataOutputView target) throws IOException {
        target.writeUTF(serialize(record));
    }

    @Override
    public BsonValue deserialize(DataInputView source) throws IOException {
        return deserialize(source.readUTF());
    }

    @Override
    public BsonValue deserialize(BsonValue reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        BsonValue tmp = deserialize(source);
        serialize(tmp, target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    private String serialize(BsonValue value) {
        return wrapBsonValue(value).toJson();
    }

    private BsonValue deserialize(String json) {
        return BsonDocument.parse(json).get(WRAPPED_KEY);
    }

    private BsonDocument wrapBsonValue(BsonValue value) {
        return new BsonDocument(WRAPPED_KEY, value);
    }

    @Override
    public TypeSerializerSnapshot<BsonValue> snapshotConfiguration() {
        return new BsonValueSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    public static final class BsonValueSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<BsonValue> {

        public BsonValueSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
