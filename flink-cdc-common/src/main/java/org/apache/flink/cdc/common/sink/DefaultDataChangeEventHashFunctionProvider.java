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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.RecordData.FieldGetter;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** The default {@link HashFunctionProvider} implementation for data change event. */
public class DefaultDataChangeEventHashFunctionProvider
        implements HashFunctionProvider<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        return new DefaultDataChangeEventHashFunction(schema);
    }

    /** The default {@link HashFunction} implementation for data change event. */
    static class DefaultDataChangeEventHashFunction implements HashFunction<DataChangeEvent> {

        private final List<FieldGetter> primaryKeyGetters;

        public DefaultDataChangeEventHashFunction(Schema schema) {
            primaryKeyGetters = createFieldGetters(schema);
        }

        @Override
        public int hashcode(DataChangeEvent event) {
            List<Object> objectsToHash = new ArrayList<>();
            // Table ID
            TableId tableId = event.tableId();
            Optional.ofNullable(tableId.getNamespace()).ifPresent(objectsToHash::add);
            Optional.ofNullable(tableId.getSchemaName()).ifPresent(objectsToHash::add);
            objectsToHash.add(tableId.getTableName());

            // Primary key
            RecordData data =
                    event.op().equals(OperationType.DELETE) ? event.before() : event.after();
            for (FieldGetter primaryKeyGetter : primaryKeyGetters) {
                objectsToHash.add(primaryKeyGetter.getFieldOrNull(data));
            }

            // Calculate hash
            return (Objects.hash(objectsToHash.toArray()) * 31) & 0x7FFFFFFF;
        }

        private List<FieldGetter> createFieldGetters(Schema schema) {
            List<FieldGetter> fieldGetters = new ArrayList<>(schema.primaryKeys().size());
            schema.primaryKeys().stream()
                    .mapToInt(
                            pk -> {
                                int index = schema.getColumnNames().indexOf(pk);
                                if (index == -1) {
                                    throw new IllegalStateException(
                                            String.format(
                                                    "Unable to find column \"%s\" which is defined as primary key",
                                                    pk));
                                }
                                return index;
                            })
                    .forEach(
                            primaryKeyPosition ->
                                    fieldGetters.add(
                                            RecordData.createFieldGetter(
                                                    schema.getColumns()
                                                            .get(primaryKeyPosition)
                                                            .getType(),
                                                    primaryKeyPosition)));
            return fieldGetters;
        }
    }
}
