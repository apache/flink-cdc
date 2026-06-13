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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbPathUtils;

import javax.annotation.Nullable;

import java.util.Objects;

/** Hashes data change events by target Lance dataset path. */
public class LanceDbHashFunctionProvider implements HashFunctionProvider<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    private final LanceDbDataSinkConfig config;

    public LanceDbHashFunctionProvider(LanceDbDataSinkConfig config) {
        this.config = config;
    }

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        return new LanceDbHashFunction(config, tableId);
    }

    private static class LanceDbHashFunction implements HashFunction<DataChangeEvent> {

        private final LanceDbDataSinkConfig config;
        private final TableId tableId;

        private LanceDbHashFunction(LanceDbDataSinkConfig config, @Nullable TableId tableId) {
            this.config = config;
            this.tableId = tableId;
        }

        @Override
        public int hashcode(DataChangeEvent event) {
            TableId effectiveTableId = tableId == null ? event.tableId() : tableId;
            String datasetPath = LanceDbPathUtils.resolveDatasetPath(effectiveTableId, config);
            return (Objects.hash(datasetPath) * 31) & 0x7FFFFFFF;
        }
    }
}
