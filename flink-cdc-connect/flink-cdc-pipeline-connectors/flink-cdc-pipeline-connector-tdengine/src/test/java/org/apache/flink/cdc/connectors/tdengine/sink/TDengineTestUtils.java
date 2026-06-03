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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineColumnDescription;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineSchemaValidator;
import org.apache.flink.cdc.connectors.tdengine.utils.TDengineTableInfo;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared TDengine connector test fixtures. */
public class TDengineTestUtils {

    public static final TableId TABLE_ID = TableId.parse("inventory.metrics");

    public static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("ts", DataTypes.BIGINT().notNull())
                    .physicalColumn("device_id", DataTypes.STRING().notNull())
                    .physicalColumn("location", DataTypes.STRING())
                    .physicalColumn("temperature", DataTypes.DOUBLE())
                    .physicalColumn("status", DataTypes.STRING())
                    .build();

    private TDengineTestUtils() {}

    public static TDengineDataSinkConfig defaultConfig() {
        return config(100, 900_000, 0, Duration.ZERO, "reject", "upsert", "reject", "reject");
    }

    public static TDengineDataSinkConfig config(
            int flushMaxRows,
            int maxSqlBytes,
            int maxRetries,
            Duration retryBackoff,
            String deleteMode,
            String updateMode,
            String timestampChangeMode,
            String subtableChangeMode) {
        return new TDengineDataSinkConfig(
                "jdbc:TAOS-WS://localhost:6041",
                "root",
                "taosdata",
                "test_db",
                "metrics",
                Collections.emptyMap(),
                true,
                "ts",
                "device_id",
                Collections.singletonList("location"),
                true,
                true,
                true,
                256,
                "nchar",
                "double",
                ZoneId.of("UTC"),
                flushMaxRows,
                maxSqlBytes,
                Duration.ofSeconds(10),
                maxRetries,
                retryBackoff,
                deleteMode,
                updateMode,
                timestampChangeMode,
                subtableChangeMode,
                Collections.emptyMap());
    }

    public static TDengineDataSinkConfig configWithMappings(Map<TableId, String> mappings) {
        return configWithMappings(mappings, 0);
    }

    public static TDengineDataSinkConfig configWithMappings(
            Map<TableId, String> mappings, int maxRetries) {
        return new TDengineDataSinkConfig(
                "jdbc:TAOS-WS://localhost:6041",
                "root",
                "taosdata",
                "test_db",
                "",
                mappings,
                true,
                "ts",
                "device_id",
                Collections.singletonList("location"),
                true,
                true,
                true,
                256,
                "nchar",
                "double",
                ZoneId.of("UTC"),
                100,
                900_000,
                Duration.ofSeconds(10),
                maxRetries,
                Duration.ZERO,
                "reject",
                "upsert",
                "reject",
                "reject",
                Collections.emptyMap());
    }

    public static RecordData record(long ts, String deviceId, String location, double temperature) {
        return GenericRecordData.of(
                ts,
                BinaryStringData.fromString(deviceId),
                BinaryStringData.fromString(location),
                temperature,
                BinaryStringData.fromString("ok"));
    }

    public static void registerExpectedStableDescription(
            RecordingClient client,
            String stableName,
            Schema schema,
            TDengineDataSinkConfig config) {
        List<TDengineColumnDescription> columns = new ArrayList<>();
        TDengineSchemaValidator.expectedColumns(schema, config)
                .forEach(
                        (name, type) ->
                                columns.add(
                                        new TDengineColumnDescription(
                                                name, type, config.getTagFields().contains(name))));
        client.stableDescriptions.put(stableName, columns);
    }

    public static class RecordingClient implements TDengineClientWrapper {

        public final List<String> sqls = new java.util.ArrayList<>();
        public final Map<String, List<TDengineColumnDescription>> stableDescriptions =
                new HashMap<>();
        public int remainingFailures;
        public int failOnInvocation = -1;
        public boolean transientFailure = true;
        public int validChecks;
        public int closeCount;
        private int executeInvocations;

        @Override
        public void execute(String sql) throws java.sql.SQLException {
            executeInvocations++;
            if (remainingFailures > 0 || executeInvocations == failOnInvocation) {
                if (remainingFailures > 0) {
                    remainingFailures--;
                }
                if (transientFailure) {
                    throw new java.sql.SQLException("connection timeout", "08006");
                }
                throw new java.sql.SQLException("syntax error", "42000");
            }
            sqls.add(sql);
        }

        @Override
        public List<TDengineColumnDescription> describeStable(TDengineTableInfo tableInfo)
                throws java.sql.SQLException {
            List<TDengineColumnDescription> columns =
                    stableDescriptions.get(tableInfo.getStableName());
            if (columns == null) {
                throw new java.sql.SQLException(
                        "Missing describe metadata for stable " + tableInfo.getStableName());
            }
            return columns;
        }

        @Override
        public boolean isValid() {
            validChecks++;
            return true;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
