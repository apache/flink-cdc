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

package com.ververica.cdc.runtime.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.runtime.partitioning.PartitioningEvent;
import com.ververica.cdc.runtime.serializer.event.PartitioningEventSerializer;

/** Type information for {@link PartitioningEvent}. */
@Internal
public class PartitioningEventTypeInfo extends TypeInformation<PartitioningEvent> {

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    public int getTotalFields() {
        return 2;
    }

    @Override
    public Class<PartitioningEvent> getTypeClass() {
        return PartitioningEvent.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<PartitioningEvent> createSerializer(ExecutionConfig config) {
        return PartitioningEventSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "PartitioningEvent";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PartitioningEvent;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof PartitioningEvent;
    }
}
