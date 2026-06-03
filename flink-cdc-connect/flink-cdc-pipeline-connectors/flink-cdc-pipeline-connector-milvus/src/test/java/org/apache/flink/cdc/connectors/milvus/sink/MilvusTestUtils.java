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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Test helpers for Milvus connector tests. */
public class MilvusTestUtils {

    private MilvusTestUtils() {}

    public static MilvusDataSinkConfig defaultConfig() {
        return config(
                Collections.emptyMap(),
                Collections.singletonList(MilvusVectorFieldSpec.parse("embedding:FloatVector(3)")),
                Collections.emptyMap(),
                100,
                Duration.ofSeconds(10),
                0,
                Duration.ZERO,
                true);
    }

    public static MilvusDataSinkConfig config(
            Map<TableId, String> collectionMappings,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled) {
        return config(
                "http://localhost:19530",
                collectionMappings,
                vectorFields,
                tableVectorFields,
                flushMaxRows,
                flushInterval,
                maxRetries,
                retryBackoff,
                deleteEnabled,
                true,
                "");
    }

    public static MilvusDataSinkConfig config(
            String uri,
            Map<TableId, String> collectionMappings,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled,
            boolean createIndexEnabled,
            String consistencyLevel) {
        return config(
                uri,
                collectionMappings,
                vectorFields,
                tableVectorFields,
                flushMaxRows,
                flushInterval,
                maxRetries,
                retryBackoff,
                deleteEnabled,
                createIndexEnabled,
                consistencyLevel,
                "reject");
    }

    public static MilvusDataSinkConfig config(
            String uri,
            Map<TableId, String> collectionMappings,
            List<MilvusVectorFieldSpec> vectorFields,
            Map<TableId, List<MilvusVectorFieldSpec>> tableVectorFields,
            int flushMaxRows,
            Duration flushInterval,
            int maxRetries,
            Duration retryBackoff,
            boolean deleteEnabled,
            boolean createIndexEnabled,
            String consistencyLevel,
            String primaryKeyChangeMode) {
        return new MilvusDataSinkConfig(
                uri,
                "",
                Duration.ofSeconds(10),
                Duration.ofSeconds(60),
                "default",
                collectionMappings,
                true,
                true,
                createIndexEnabled,
                false,
                false,
                "",
                false,
                1024,
                Collections.emptyList(),
                vectorFields,
                tableVectorFields,
                "",
                65535,
                flushMaxRows,
                flushInterval,
                maxRetries,
                retryBackoff,
                true,
                10,
                deleteEnabled,
                primaryKeyChangeMode,
                false,
                consistencyLevel,
                "AUTOINDEX",
                "COSINE",
                Collections.emptyMap());
    }
}
