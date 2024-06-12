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

package org.apache.flink.cdc.runtime.serializer.event;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.event.AlterColumnCommentEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.runtime.serializer.MapSerializer;
import org.apache.flink.cdc.runtime.serializer.StringSerializer;
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Collections;

/** A {@link TypeSerializer} for {@link AlterColumnCommentEvent}. */
public class AlterColumnCommentEventSerializer
        extends TypeSerializerSingleton<AlterColumnCommentEvent> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the TableIdSerializer. */
    public static final AlterColumnCommentEventSerializer INSTANCE =
            new AlterColumnCommentEventSerializer();

    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final MapSerializer<String, String> commentMapSerializer =
            new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AlterColumnCommentEvent createInstance() {
        return new AlterColumnCommentEvent(TableId.tableId("unknown"), Collections.emptyMap());
    }

    @Override
    public AlterColumnCommentEvent copy(AlterColumnCommentEvent from) {
        return new AlterColumnCommentEvent(
                from.tableId(),
                commentMapSerializer.copy(from.getCommentMapping()),
                commentMapSerializer.copy(from.getOldCommentMapping()));
    }

    @Override
    public AlterColumnCommentEvent copy(
            AlterColumnCommentEvent from, AlterColumnCommentEvent reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AlterColumnCommentEvent record, DataOutputView target)
            throws IOException {
        tableIdSerializer.serialize(record.tableId(), target);
        commentMapSerializer.serialize(record.getCommentMapping(), target);
        commentMapSerializer.serialize(record.getOldCommentMapping(), target);
    }

    @Override
    public AlterColumnCommentEvent deserialize(DataInputView source) throws IOException {
        return new AlterColumnCommentEvent(
                tableIdSerializer.deserialize(source),
                commentMapSerializer.deserialize(source),
                commentMapSerializer.deserialize(source));
    }

    @Override
    public AlterColumnCommentEvent deserialize(AlterColumnCommentEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<AlterColumnCommentEvent> snapshotConfiguration() {
        return new AlterColumnCommentEventSerializer.AlterColumnCommentEventSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class AlterColumnCommentEventSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AlterColumnCommentEvent> {

        public AlterColumnCommentEventSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
