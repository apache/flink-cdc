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

package org.apache.flink.cdc.connectors.values.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** A test for the {@link org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkHelper}. */
class ValuesDataSinkHelperTest {

    @Test
    void testConvertEventToStr() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        TableId tableId = TableId.parse("default.default.table1");
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));

        List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(schema);
        Assertions.assertThat(
                        ValuesDataSinkHelper.convertEventToStr(
                                new CreateTableEvent(tableId, schema), fieldGetters))
                .isEqualTo(
                        "CreateTableEvent{tableId=default.default.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}");

        DataChangeEvent insertEvent =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        Assertions.assertThat(ValuesDataSinkHelper.convertEventToStr(insertEvent, fieldGetters))
                .isEqualTo(
                        "DataChangeEvent{tableId=default.default.table1, before=[], after=[1, 1], op=INSERT, meta=()}");
        DataChangeEvent deleteEvent =
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        Assertions.assertThat(ValuesDataSinkHelper.convertEventToStr(deleteEvent, fieldGetters))
                .isEqualTo(
                        "DataChangeEvent{tableId=default.default.table1, before=[1, 1], after=[], op=DELETE, meta=()}");
        DataChangeEvent updateEvent =
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }),
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("x")
                                }));
        Assertions.assertThat(ValuesDataSinkHelper.convertEventToStr(updateEvent, fieldGetters))
                .isEqualTo(
                        "DataChangeEvent{tableId=default.default.table1, before=[1, 1], after=[1, x], op=UPDATE, meta=()}");
    }
}
