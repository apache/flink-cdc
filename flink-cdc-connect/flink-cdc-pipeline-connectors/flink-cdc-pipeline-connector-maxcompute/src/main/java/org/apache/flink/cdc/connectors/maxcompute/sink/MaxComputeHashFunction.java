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

package org.apache.flink.cdc.connectors.maxcompute.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;

import com.aliyun.odps.tunnel.hasher.TypeHasher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Hash function for maxcompute to distribute data change event to different maxcompute sink by
 * primary key.
 */
public class MaxComputeHashFunction implements Function<DataChangeEvent, Integer> {

    private final int bucketSize;
    private final List<RecordData.FieldGetter> primaryKeyGetters;

    public MaxComputeHashFunction(Schema schema, int bucketSize) {
        primaryKeyGetters = createFieldGetters(schema);
        this.bucketSize = bucketSize;
    }

    @Override
    public Integer apply(DataChangeEvent event) {
        List<Integer> hashes = new ArrayList<>();
        RecordData data = event.op().equals(OperationType.DELETE) ? event.before() : event.after();
        for (RecordData.FieldGetter primaryKeyGetter : primaryKeyGetters) {
            Object object = primaryKeyGetter.getFieldOrNull(data);
            int hash = TypeHasher.hash(TypeConvertUtils.inferMaxComputeType(object), object);
            hashes.add(hash);
        }
        return TypeHasher.CombineHashVal(hashes) % bucketSize;
    }

    private List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(schema.primaryKeys().size());
        int[] primaryKeyPositions =
                schema.primaryKeys().stream()
                        .mapToInt(
                                pk -> {
                                    int i = 0;
                                    while (!schema.getColumns().get(i).getName().equals(pk)) {
                                        ++i;
                                    }
                                    if (i >= schema.getColumnCount()) {
                                        throw new IllegalStateException(
                                                String.format(
                                                        "Unable to find column \"%s\" which is defined as primary key",
                                                        pk));
                                    }
                                    return i;
                                })
                        .toArray();
        for (int primaryKeyPosition : primaryKeyPositions) {
            fieldGetters.add(
                    TypeConvertUtils.createFieldGetter(
                            schema.getColumns().get(primaryKeyPosition).getType(),
                            primaryKeyPosition));
        }
        return fieldGetters;
    }
}
