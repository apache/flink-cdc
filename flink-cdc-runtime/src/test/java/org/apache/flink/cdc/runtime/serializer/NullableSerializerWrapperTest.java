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

package org.apache.flink.cdc.runtime.serializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;

/** A test for the {@link NullableSerializerWrapper}. */
class NullableSerializerWrapperTest extends SerializerTestBase<Long> {

    @Override
    protected TypeSerializer<Long> createSerializer() {
        return new NullableSerializerWrapper<>(LongSerializer.INSTANCE);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Class<Long> getTypeClass() {
        return Long.class;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected Long[] getTestData() {
        return new Long[] {1L, null};
    }
}
