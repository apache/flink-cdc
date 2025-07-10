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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.time.LocalDate;

/** A test for the {@link TimestampDataSerializer}. */
class DateDataSerializerTest extends SerializerTestBase<DateData> {
    @Override
    protected TypeSerializer<DateData> createSerializer() {
        return DateDataSerializer.INSTANCE;
    }

    @Override
    protected int getLength() {
        return 4;
    }

    @Override
    protected Class<DateData> getTypeClass() {
        return DateData.class;
    }

    @Override
    protected DateData[] getTestData() {
        return new DateData[] {
            DateData.fromEpochDay(10240),
            DateData.fromEpochDay(20480),
            DateData.fromEpochDay(40960),
            DateData.fromIsoLocalDateString("2014-08-15"),
            DateData.fromIsoLocalDateString("2001-03-15"),
            DateData.fromIsoLocalDateString("2023-09-19"),
            DateData.fromLocalDate(LocalDate.of(2012, 12, 22)),
            DateData.fromLocalDate(LocalDate.of(1999, 12, 31))
        };
    }
}
