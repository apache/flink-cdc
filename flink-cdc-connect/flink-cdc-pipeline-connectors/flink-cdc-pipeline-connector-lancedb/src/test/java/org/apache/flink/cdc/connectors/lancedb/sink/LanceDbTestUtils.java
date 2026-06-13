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

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.lancedb.client.LanceDbClient;

import org.apache.arrow.vector.types.pojo.Field;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared LanceDB connector test fixtures. */
public class LanceDbTestUtils {

    public static final TableId TABLE_ID = TableId.parse("inventory.products");

    public static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT().notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("price", DataTypes.DOUBLE())
                    .build();

    private LanceDbTestUtils() {}

    public static LanceDbDataSinkConfig defaultConfig() {
        return config("append-only", 100, 0, Duration.ZERO, false);
    }

    public static LanceDbDataSinkConfig config(
            String changelogMode,
            int flushMaxRows,
            int maxRetries,
            Duration retryBackoff,
            boolean schemaEvolutionEnabled) {
        return config(
                changelogMode,
                flushMaxRows,
                flushMaxRows,
                maxRetries,
                retryBackoff,
                schemaEvolutionEnabled);
    }

    public static LanceDbDataSinkConfig config(
            String changelogMode,
            int flushMaxRows,
            int maxRowsPerCommit,
            int maxRetries,
            Duration retryBackoff,
            boolean schemaEvolutionEnabled) {
        return new LanceDbDataSinkConfig(
                "/tmp/lancedb",
                Collections.emptyMap(),
                true,
                true,
                true,
                schemaEvolutionEnabled,
                changelogMode,
                flushMaxRows,
                maxRowsPerCommit,
                Duration.ofSeconds(10),
                maxRetries,
                retryBackoff,
                1024,
                128,
                1024 * 1024L,
                true,
                "CREATE",
                ZoneId.of("UTC"),
                Collections.emptyMap());
    }

    public static RecordData record(int id, String name, double price) {
        return GenericRecordData.of(id, BinaryStringData.fromString(name), price);
    }

    public static class RecordingClient implements LanceDbClient {

        final Map<String, org.apache.arrow.vector.types.pojo.Schema> schemas = new HashMap<>();
        final List<AppendCall> appendCalls = new ArrayList<>();
        final List<String> reopened = new ArrayList<>();
        int remainingFailures;
        int failOnAppendCall = -1;
        boolean retryableFailure = true;
        int closeCount;

        @Override
        public boolean datasetExists(String datasetPath) {
            return schemas.containsKey(datasetPath);
        }

        @Override
        public org.apache.arrow.vector.types.pojo.Schema getSchema(String datasetPath) {
            return schemas.get(datasetPath);
        }

        @Override
        public void createDataset(
                String datasetPath, org.apache.arrow.vector.types.pojo.Schema schema) {
            schemas.put(datasetPath, schema);
        }

        @Override
        public void addColumns(String datasetPath, List<Field> fields) {
            org.apache.arrow.vector.types.pojo.Schema schema = schemas.get(datasetPath);
            List<Field> next = new ArrayList<>(schema.getFields());
            next.addAll(fields);
            schemas.put(datasetPath, new org.apache.arrow.vector.types.pojo.Schema(next));
        }

        @Override
        public int appendRows(
                String datasetPath,
                org.apache.arrow.vector.types.pojo.Schema schema,
                List<List<Object>> rows) {
            int attemptNumber = appendCalls.size() + 1;
            if (failOnAppendCall == attemptNumber) {
                failOnAppendCall = -1;
                throw new RuntimeException("temporary commit conflict");
            }
            if (remainingFailures > 0) {
                remainingFailures--;
                if (retryableFailure) {
                    throw new RuntimeException("temporary commit conflict");
                }
                throw new RuntimeException("invalid schema");
            }
            appendCalls.add(new AppendCall(datasetPath, schema, rows));
            return rows.size();
        }

        @Override
        public void reopen(String datasetPath) {
            reopened.add(datasetPath);
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    public static class AppendCall {
        final String datasetPath;
        final org.apache.arrow.vector.types.pojo.Schema schema;
        final List<List<Object>> rows;

        AppendCall(
                String datasetPath,
                org.apache.arrow.vector.types.pojo.Schema schema,
                List<List<Object>> rows) {
            this.datasetPath = datasetPath;
            this.schema = schema;
            this.rows = new ArrayList<>(rows);
        }
    }
}
