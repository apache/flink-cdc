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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for converting CDC rows to native DWS records. */
class DwsRecordConverterTest {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("tenant", DataTypes.INT().notNull())
                    .physicalColumn("id", DataTypes.VARBINARY(16).notNull())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("amount", DataTypes.DECIMAL(12, 2))
                    .physicalColumn("created_at", DataTypes.TIMESTAMP(3))
                    .physicalColumn("observed_at", DataTypes.TIMESTAMP_LTZ(3))
                    .physicalColumn("optional_note", DataTypes.STRING())
                    .primaryKey("tenant", "id")
                    .build();

    @Test
    void convertsCompleteRowsWithoutSilentlyChangingValues() {
        DwsRecordConverter converter = new DwsRecordConverter(SCHEMA, ZoneId.of("Asia/Shanghai"));
        byte[] id = new byte[] {0, 1, (byte) 0xff};
        GenericRecordData row =
                GenericRecordData.of(
                        7,
                        id,
                        BinaryStringData.fromString("中文\u0000name"),
                        DecimalData.fromBigDecimal(new BigDecimal("123456.70"), 12, 2),
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2026-10-10T01:02:03.456")),
                        LocalZonedTimestampData.fromInstant(
                                Instant.parse("2026-10-10T01:02:03.456Z")),
                        null);

        Map<String, Object> converted = converter.convertWrite(row);

        assertThat(converted)
                .containsEntry("tenant", 7)
                .containsEntry("name", "中文\u0000name")
                .containsEntry("amount", new BigDecimal("123456.70"))
                .containsEntry("created_at", Timestamp.valueOf("2026-10-10 01:02:03.456"))
                .containsEntry("observed_at", Timestamp.valueOf("2026-10-10 09:02:03.456"))
                .containsEntry("optional_note", null);
        assertThat((byte[]) converted.get("id")).containsExactly(id);
        assertThat(converted).hasSize(SCHEMA.getColumns().size());
    }

    @Test
    void convertsDeletesToCompositePrimaryKeyOnly() {
        DwsRecordConverter converter = new DwsRecordConverter(SCHEMA, ZoneId.of("UTC"));
        byte[] id = new byte[] {3, 4};
        GenericRecordData row =
                GenericRecordData.of(
                        9,
                        id,
                        BinaryStringData.fromString("must-not-be-written"),
                        DecimalData.fromBigDecimal(new BigDecimal("1.00"), 12, 2),
                        TimestampData.fromLocalDateTime(LocalDateTime.parse("2026-10-10T01:02:03")),
                        LocalZonedTimestampData.fromInstant(Instant.EPOCH),
                        null);

        Map<String, Object> converted = converter.convertDelete(row);

        assertThat(converted).containsOnlyKeys("tenant", "id").containsEntry("tenant", 9);
        assertThat((byte[]) converted.get("id")).containsExactly(id);
    }
}
