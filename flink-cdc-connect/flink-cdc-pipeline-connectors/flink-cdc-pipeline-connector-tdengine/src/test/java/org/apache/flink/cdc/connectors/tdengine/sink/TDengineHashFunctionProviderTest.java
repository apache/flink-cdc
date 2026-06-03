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

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.function.HashFunction;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link TDengineHashFunctionProvider}. */
class TDengineHashFunctionProviderTest {

    @Test
    void testHashesSameSubtableToSameBucket() {
        HashFunction<DataChangeEvent> hashFunction =
                new TDengineHashFunctionProvider(TDengineTestUtils.defaultConfig())
                        .getHashFunction(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA);

        int first =
                hashFunction.hashcode(
                        DataChangeEvent.insertEvent(
                                TDengineTestUtils.TABLE_ID,
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D)));
        int second =
                hashFunction.hashcode(
                        DataChangeEvent.updateEvent(
                                TDengineTestUtils.TABLE_ID,
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D),
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 13.5D)));

        Assertions.assertThat(second).isEqualTo(first);
    }

    @Test
    void testHashesDifferentSubtablesDifferently() {
        HashFunction<DataChangeEvent> hashFunction =
                new TDengineHashFunctionProvider(TDengineTestUtils.defaultConfig())
                        .getHashFunction(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA);

        int first =
                hashFunction.hashcode(
                        DataChangeEvent.insertEvent(
                                TDengineTestUtils.TABLE_ID,
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D)));
        int second =
                hashFunction.hashcode(
                        DataChangeEvent.insertEvent(
                                TDengineTestUtils.TABLE_ID,
                                TDengineTestUtils.record(1000L, "device-2", "room-a", 12.5D)));

        Assertions.assertThat(second).isNotEqualTo(first);
    }

    @Test
    void testHashesNormalizedSubtableName() {
        HashFunction<DataChangeEvent> hashFunction =
                new TDengineHashFunctionProvider(TDengineTestUtils.defaultConfig())
                        .getHashFunction(TDengineTestUtils.TABLE_ID, TDengineTestUtils.SCHEMA);

        int hyphenated =
                hashFunction.hashcode(
                        DataChangeEvent.insertEvent(
                                TDengineTestUtils.TABLE_ID,
                                TDengineTestUtils.record(1000L, "device-1", "room-a", 12.5D)));
        int normalized =
                hashFunction.hashcode(
                        DataChangeEvent.insertEvent(
                                TDengineTestUtils.TABLE_ID,
                                TDengineTestUtils.record(1000L, "device_1", "room-a", 12.5D)));

        Assertions.assertThat(normalized).isEqualTo(hyphenated);
    }
}
