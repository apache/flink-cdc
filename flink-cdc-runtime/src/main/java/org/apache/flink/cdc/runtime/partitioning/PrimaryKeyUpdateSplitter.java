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

package org.apache.flink.cdc.runtime.partitioning;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Splits primary-key-changing update events before they are partitioned. */
final class PrimaryKeyUpdateSplitter {

    private final List<RecordData.FieldGetter> primaryKeyGetters;

    PrimaryKeyUpdateSplitter(Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException(
                    "Primary-key update splitting requires at least one primary key column");
        }
        primaryKeyGetters = new ArrayList<>(schema.primaryKeys().size());
        for (String primaryKey : schema.primaryKeys()) {
            int position = schema.getColumnNames().indexOf(primaryKey);
            if (position < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unable to find column \"%s\" which is defined as primary key",
                                primaryKey));
            }
            primaryKeyGetters.add(
                    RecordData.createFieldGetter(
                            schema.getColumns().get(position).getType(), position));
        }
    }

    List<DataChangeEvent> split(DataChangeEvent event) {
        if (event.op() != OperationType.UPDATE) {
            return Collections.singletonList(event);
        }
        if (event.before() == null) {
            throw new IllegalArgumentException("UPDATE event is missing its before record");
        }
        if (event.after() == null) {
            throw new IllegalArgumentException("UPDATE event is missing its after record");
        }
        if (!hasPrimaryKeyChanged(event.before(), event.after())) {
            return Collections.singletonList(event);
        }
        return Arrays.asList(
                DataChangeEvent.updateBeforeEvent(event.tableId(), event.before(), event.meta()),
                DataChangeEvent.replaceEvent(event.tableId(), event.after(), event.meta()));
    }

    private boolean hasPrimaryKeyChanged(RecordData before, RecordData after) {
        for (RecordData.FieldGetter getter : primaryKeyGetters) {
            if (!Objects.deepEquals(getter.getFieldOrNull(before), getter.getFieldOrNull(after))) {
                return true;
            }
        }
        return false;
    }
}
