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

package org.apache.flink.cdc.common.data;

import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link ArrayData}. */
class ArrayDataTest {

    @Test
    void testElementGetterHonorsRuntimeNullForNotNullDecimalElement() {
        AtomicBoolean decimalGetterInvoked = new AtomicBoolean(false);
        ArrayData arrayData =
                (ArrayData)
                        Proxy.newProxyInstance(
                                ArrayData.class.getClassLoader(),
                                new Class[] {ArrayData.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "size":
                                            return 1;
                                        case "isNullAt":
                                            return true;
                                        case "getDecimal":
                                            decimalGetterInvoked.set(true);
                                            fail(
                                                    "Decimal accessor should not be called for null array elements");
                                            return null;
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });

        ArrayData.ElementGetter elementGetter =
                ArrayData.createElementGetter(DataTypes.DECIMAL(20, 0).notNull());

        assertThat(elementGetter.getElementOrNull(arrayData, 0)).isNull();
        assertThat(decimalGetterInvoked).isFalse();
    }
}
