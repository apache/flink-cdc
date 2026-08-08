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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussSplitReader}. */
class FlussSplitReaderTest {

    private static final long TABLE_ID = 1001L;
    private static final long OTHER_TABLE_ID = 2002L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final PhysicalTablePath PHYSICAL_TABLE_PATH = PhysicalTablePath.of(TABLE_PATH);
    private static final TableBucket TABLE_BUCKET = new TableBucket(TABLE_ID, 0);

    @Test
    void testValidateLogSplitTableId() {
        assertTableIdValidation(new FlussLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 100L));
    }

    @Test
    void testValidateHybridSnapshotLogSplitTableId() {
        assertTableIdValidation(
                new FlussHybridSnapshotLogSplit(PHYSICAL_TABLE_PATH, TABLE_BUCKET, 10L, 100L));
    }

    private static void assertTableIdValidation(FlussSplitBase split) {
        assertThatCode(() -> FlussSplitReader.validateTableId(split, TABLE_ID))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> FlussSplitReader.validateTableId(split, OTHER_TABLE_ID))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Table ID mismatch for split test_db.test_table.0: split table ID is 1001, but table test_db.test_table has ID 2002.");
    }
}
