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

package org.apache.flink.cdc.connectors.tdengine.utils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link TDengineNameUtils}. */
class TDengineNameUtilsTest {

    @Test
    void testNormalizeIdentifier() {
        Assertions.assertThat(TDengineNameUtils.normalizeIdentifier("9-device.name", "subtable"))
                .isEqualTo("_9_device_name");
    }

    @Test
    void testRejectInvalidIdentifierWhenNormalizationDisabled() {
        Assertions.assertThatThrownBy(
                        () -> TDengineNameUtils.validateIdentifier("device-name", "subtable"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[A-Za-z_][A-Za-z0-9_]*");
    }
}
