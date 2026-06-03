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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.milvus.serde.MilvusRowConverter;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusCollectionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Hashes data change events by target collection and Milvus primary key. */
public class MilvusHashFunctionProvider implements HashFunctionProvider<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    private final MilvusDataSinkConfig config;

    public MilvusHashFunctionProvider(MilvusDataSinkConfig config) {
        this.config = config;
    }

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        return new MilvusHashFunction(config, schema);
    }

    private static class MilvusHashFunction implements HashFunction<DataChangeEvent> {

        private final MilvusDataSinkConfig config;
        private final String primaryKey;
        private final MilvusRowConverter primaryKeyConverter;

        private MilvusHashFunction(MilvusDataSinkConfig config, Schema schema) {
            this.config = config;
            this.primaryKey = MilvusCollectionUtils.resolvePrimaryKey(schema, config);
            this.primaryKeyConverter =
                    new MilvusRowConverter(
                            schema, Collections.emptyList(), config.getVarcharMaxLengthDefault());
        }

        @Override
        public int hashcode(DataChangeEvent event) {
            String collectionName =
                    MilvusCollectionUtils.resolveCollectionName(event.tableId(), config);
            if ("allow".equalsIgnoreCase(config.getPrimaryKeyChangeMode())) {
                return (Objects.hash(collectionName) * 31) & 0x7FFFFFFF;
            }
            RecordData keyRecord =
                    event.op() == OperationType.DELETE ? event.before() : event.after();
            if (keyRecord == null) {
                throw new IllegalStateException(
                        event.op() + " event misses the record used for Milvus hash routing.");
            }
            Object primaryKeyValue = primaryKeyConverter.extractPrimaryKey(keyRecord, primaryKey);

            List<Object> objectsToHash = new ArrayList<>();
            objectsToHash.add(collectionName);
            objectsToHash.add(primaryKeyValue);
            return (Objects.hash(objectsToHash.toArray()) * 31) & 0x7FFFFFFF;
        }
    }
}
