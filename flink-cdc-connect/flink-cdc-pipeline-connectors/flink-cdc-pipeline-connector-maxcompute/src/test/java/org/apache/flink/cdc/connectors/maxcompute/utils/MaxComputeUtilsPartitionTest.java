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

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.maxcompute.EmulatorTestBase;
import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.options.CompressAlgorithm;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.writer.MaxComputeWriter;

import com.aliyun.odps.Instance;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Emulator-based tests for partition handling. Note that the Emulator only supports uppercase input
 * (However, MaxCompute can correctly distinguish between uppercase and lowercase).
 */
class MaxComputeUtilsPartitionTest extends EmulatorTestBase {

    private static final String TEST_TABLE = "PARTITION_WRITE_TEST_TABLE";

    @AfterEach
    void deleteTable() throws Exception {
        odpsInstance.tables().delete(TEST_TABLE, true);
    }

    /**
     * Verifies the partitioned-table write path used by the pipeline job: create the partitioned
     * table, create the partition if absent, write records through the session writer, and read
     * them back. The partition key is deliberately placed as the first column to cover partition
     * columns that are not the last column.
     */
    @Test
    void testCreatePartitionIfAbsentAndWritePartitionedTable() throws Exception {
        SchemaEvolutionUtils.createTable(
                testOptions,
                TableId.tableId(TEST_TABLE),
                Schema.newBuilder()
                        .physicalColumn("PT", DataTypes.STRING())
                        .physicalColumn("COL1", DataTypes.STRING())
                        .physicalColumn("COL2", DataTypes.STRING())
                        .partitionKey("PT")
                        .primaryKey("COL1")
                        .build());

        // must not throw, and must be idempotent
        MaxComputeUtils.createPartitionIfAbsent(testOptions, null, TEST_TABLE, "PT='2024-01'");
        MaxComputeUtils.createPartitionIfAbsent(testOptions, null, TEST_TABLE, "PT='2024-01'");

        writeRow("2024-01", "1", "a");
        writeRow("2024-01", "2", "b");
        writeRow("2024-02", "3", "c");

        Instance instance =
                SQLTask.run(
                        odpsInstance,
                        "select PT, COL1, COL2 from " + TEST_TABLE + " order by COL1;");
        instance.waitForSuccess();
        List<Record> result = SQLTask.getResult(instance);
        assertThat(result).hasSize(3);
        assertThat(result.get(0).get(0)).isEqualTo("2024-01");
        assertThat(result.get(0).get(1)).isEqualTo("1");
        assertThat(result.get(0).get(2)).isEqualTo("a");
        assertThat(result.get(1).get(0)).isEqualTo("2024-01");
        assertThat(result.get(1).get(1)).isEqualTo("2");
        assertThat(result.get(1).get(2)).isEqualTo("b");
        assertThat(result.get(2).get(0)).isEqualTo("2024-02");
        assertThat(result.get(2).get(1)).isEqualTo("3");
        assertThat(result.get(2).get(2)).isEqualTo("c");
    }

    private void writeRow(String pt, String col1, String col2) throws Exception {
        // session id is null on first creation, mirroring SessionManageCoordinator#createWriter
        SessionIdentifier identifier =
                SessionIdentifier.of(testOptions.getProject(), null, TEST_TABLE, "PT='" + pt + "'");
        MaxComputeWriteOptions writeOptions =
                MaxComputeWriteOptions.builder()
                        .withCompressAlgorithm(CompressAlgorithm.RAW)
                        .build();
        MaxComputeWriter writer =
                MaxComputeWriter.batchWriter(testOptions, writeOptions, identifier);
        ArrayRecord record = writer.newElement();
        record.setString(0, col1);
        record.setString(1, col2);
        writer.write(record);
        writer.flush();
        writer.commit();
    }
}
