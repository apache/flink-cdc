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
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.runtime.serializer.SerializerTestBase;

import java.time.LocalDateTime;
import java.time.ZoneId;

/** A test for the {@link ZonedTimestampDataSerializer}. */
abstract class ZonedTimestampDataSerializerTest extends SerializerTestBase<ZonedTimestampData> {
    @Override
    protected TypeSerializer<ZonedTimestampData> createSerializer() {
        return new ZonedTimestampDataSerializer(getPrecision());
    }

    @Override
    protected int getLength() {
        return (getPrecision() <= 3) ? 8 : 12;
    }

    @Override
    protected Class<ZonedTimestampData> getTypeClass() {
        return ZonedTimestampData.class;
    }

    @Override
    protected ZonedTimestampData[] getTestData() {
        if (getPrecision() > 3) {
            return new ZonedTimestampData[] {
                ZonedTimestampData.fromZonedDateTime(
                        LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                .atZone(ZoneId.systemDefault())),
                ZonedTimestampData.fromZonedDateTime(
                        LocalDateTime.of(2023, 12, 12, 12, 12, 12, 12)
                                .atZone(ZoneId.systemDefault()))
            };
        } else {
            return new ZonedTimestampData[] {
                ZonedTimestampData.fromZonedDateTime(
                        LocalDateTime.of(2023, 11, 11, 11, 11, 11, 0)
                                .atZone(ZoneId.systemDefault())),
                ZonedTimestampData.fromZonedDateTime(
                        LocalDateTime.of(2023, 12, 12, 12, 12, 12, 0)
                                .atZone(ZoneId.systemDefault()))
            };
        }
    }

    protected abstract int getPrecision();

    static final class ZonedTimestampSerializer0Test extends ZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 0;
        }
    }

    static final class ZonedTimestampSerializer3Test extends ZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 3;
        }
    }

    static final class ZonedTimestampSerializer6Test extends ZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 6;
        }
    }

    static final class ZonedTimestampSerializer8Test extends ZonedTimestampDataSerializerTest {
        @Override
        protected int getPrecision() {
            return 8;
        }
    }
}
