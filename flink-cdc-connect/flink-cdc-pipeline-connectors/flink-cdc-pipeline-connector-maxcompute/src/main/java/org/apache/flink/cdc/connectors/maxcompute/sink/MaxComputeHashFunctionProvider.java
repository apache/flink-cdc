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
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;

import com.aliyun.odps.tunnel.hasher.TypeHasher;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Hash function for maxcompute to distribute data change event to different maxcompute sink by
 * primary key.
 */
public class MaxComputeHashFunctionProvider implements HashFunctionProvider<DataChangeEvent> {
    private static final long serialVersionUID = 1L;
    private final int bucketSize;

    public MaxComputeHashFunctionProvider(MaxComputeOptions options) {
        this.bucketSize = options.getBucketsNum();
    }

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        return new MaxComputeHashFunction(schema, bucketSize);
    }

    static class MaxComputeHashFunction implements HashFunction<DataChangeEvent> {
        private final int bucketSize;
        private final List<RecordData.FieldGetter> primaryKeyGetters;

        public MaxComputeHashFunction(Schema schema, int bucketSize) {
            primaryKeyGetters = createFieldGetters(schema);
            this.bucketSize = bucketSize;
        }

        @Override
        public int hashcode(DataChangeEvent event) {
            List<Integer> hashes = new ArrayList<>();
            RecordData data =
                    event.op().equals(OperationType.DELETE) ? event.before() : event.after();
            for (RecordData.FieldGetter primaryKeyGetter : primaryKeyGetters) {
                Object object = primaryKeyGetter.getFieldOrNull(data);
                int hash =
                        object == null
                                ? 0
                                : TypeHasher.hash(
                                        TypeConvertUtils.inferMaxComputeType(object), object);
                hashes.add(hash);
            }
            return TypeHasher.CombineHashVal(hashes) % bucketSize;
        }

        private List<RecordData.FieldGetter> createFieldGetters(Schema schema) {
            List<RecordData.FieldGetter> fieldGetters =
                    new ArrayList<>(schema.primaryKeys().size());
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
                                            TypeConvertUtils.createFieldGetter(
                                                    schema.getColumns()
                                                            .get(primaryKeyPosition)
                                                            .getType(),
                                                    primaryKeyPosition)));
            return fieldGetters;
        }
    }
}
