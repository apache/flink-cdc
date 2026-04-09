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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.runtime.testutils.operators.RegularEventOperatorTestHarness;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Regression tests for unexpected runtime null values in post-transform input records. */
class PostTransformOperatorRuntimeNullRegressionTest {

    @Test
    void testUnexpectedRuntimeNullInNotNullDecimalColumn() throws Exception {
        TableId tableId = TableId.tableId("my_company", "my_branch", "decimal_runtime_nulls");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.DECIMAL(20, 0).notNull())
                        .physicalColumn("payload", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        PostTransformOperator transform =
                PostTransformOperator.newBuilder()
                        .addTransform(
                                tableId.identifier(),
                                "id, payload",
                                null,
                                "id",
                                null,
                                null,
                                null,
                                new SupportedMetadataColumn[0])
                        .build();

        try (RegularEventOperatorTestHarness<PostTransformOperator, Event> harness =
                RegularEventOperatorTestHarness.with(transform, 1)) {
            harness.open();

            CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
            transform.processElement(new StreamRecord<>(createTableEvent));
            Assertions.assertThat(harness.getOutputRecords().poll())
                    .isEqualTo(new StreamRecord<>(createTableEvent));

            BinaryRecordDataGenerator generator =
                    new BinaryRecordDataGenerator(((RowType) schema.toRowDataType()));
            DataChangeEvent insertEvent =
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {null, BinaryStringData.fromString("payload")}));

            transform.processElement(new StreamRecord<>(insertEvent));
            Assertions.assertThat(harness.getOutputRecords().poll())
                    .isEqualTo(new StreamRecord<>(insertEvent));
        }
    }
}
