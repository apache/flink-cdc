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

package org.apache.flink.cdc.connectors.tdengine.serde;

import org.apache.flink.cdc.connectors.tdengine.sink.TDengineTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link TDengineRowConverter}. */
class TDengineRowConverterTest {

    @Test
    void testConvertSplitsTimestampSubtableTagsAndMetrics() {
        TDengineRowConverter converter =
                new TDengineRowConverter(
                        TDengineTestUtils.TABLE_ID,
                        TDengineTestUtils.SCHEMA,
                        TDengineTestUtils.defaultConfig());

        TDengineRowData row =
                converter.convert(TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D));

        Assertions.assertThat(row.getSubtableName()).isEqualTo("device_1");
        Assertions.assertThat(row.getTagColumnNames()).containsExactly("location");
        Assertions.assertThat(row.getTagValues()).containsExactly("room-a");
        Assertions.assertThat(row.getMetricColumnNames())
                .containsExactly("ts", "temperature", "status");
        Assertions.assertThat(row.getMetricValues()).containsExactly(1000L, 12.5D, "ok");
    }

    @Test
    void testSubtableEqualsUsesNormalizedName() {
        TDengineRowConverter converter =
                new TDengineRowConverter(
                        TDengineTestUtils.TABLE_ID,
                        TDengineTestUtils.SCHEMA,
                        TDengineTestUtils.defaultConfig());

        Assertions.assertThat(
                        converter.subtableEquals(
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D),
                                TDengineTestUtils.record(1000L, "device_1", "room-a", 12.5D)))
                .isTrue();
    }

    @Test
    void testTimestampEqualsDetectsChanges() {
        TDengineRowConverter converter =
                new TDengineRowConverter(
                        TDengineTestUtils.TABLE_ID,
                        TDengineTestUtils.SCHEMA,
                        TDengineTestUtils.defaultConfig());

        Assertions.assertThat(
                        converter.timestampEquals(
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D),
                                TDengineTestUtils.record(2000L, "device-1", "room-a", 12.5D)))
                .isFalse();
    }

    @Test
    void testRejectsNullTimestamp() {
        TDengineRowConverter converter =
                new TDengineRowConverter(
                        TDengineTestUtils.TABLE_ID,
                        TDengineTestUtils.SCHEMA,
                        TDengineTestUtils.defaultConfig());

        Assertions.assertThatThrownBy(
                        () ->
                                converter.convert(
                                        org.apache.flink.cdc.common.data.GenericRecordData.of(
                                                null,
                                                org.apache.flink.cdc.common.data.binary
                                                        .BinaryStringData.fromString("device-1"),
                                                org.apache.flink.cdc.common.data.binary
                                                        .BinaryStringData.fromString("room-a"),
                                                12.5D,
                                                org.apache.flink.cdc.common.data.binary
                                                        .BinaryStringData.fromString("ok"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timestamp.field");
    }
}
