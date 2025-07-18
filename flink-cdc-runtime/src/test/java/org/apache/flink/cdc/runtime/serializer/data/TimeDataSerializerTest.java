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

package org.apache.flink.cdc.runtime.serializer.data;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.time.LocalTime;

/** A test for the {@link TimestampDataSerializer}. */
class TimeDataSerializerTest extends SerializerTestBase<TimeData> {
    @Override
    protected TypeSerializer<TimeData> createSerializer() {
        return TimeDataSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return 4;
    }

    @Override
    protected Class<TimeData> getTypeClass() {
        return TimeData.class;
    }

    @Override
    protected TimeData[] getTestData() {
        return new TimeData[] {
            TimeData.fromSecondOfDay(1024),
            TimeData.fromSecondOfDay(2048),
            TimeData.fromSecondOfDay(4096),
            TimeData.fromMillisOfDay(10240),
            TimeData.fromMillisOfDay(20480),
            TimeData.fromMillisOfDay(40960),
            TimeData.fromNanoOfDay(102400),
            TimeData.fromNanoOfDay(204800),
            TimeData.fromNanoOfDay(409600),
            TimeData.fromIsoLocalTimeString("14:28:25"),
            TimeData.fromIsoLocalTimeString("01:23:45"),
            TimeData.fromIsoLocalTimeString("23:59:59"),
            TimeData.fromLocalTime(LocalTime.MIDNIGHT),
            TimeData.fromLocalTime(LocalTime.NOON)
        };
    }
}
