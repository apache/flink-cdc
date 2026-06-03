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

import java.sql.SQLException;

/** Tests for {@link TDengineRetryUtils}. */
class TDengineRetryUtilsTest {

    @Test
    void testRecognizesConnectionSqlStateAsTransient() {
        Assertions.assertThat(TDengineRetryUtils.isTransient(new SQLException("failed", "08006")))
                .isTrue();
    }

    @Test
    void testRecognizesNetworkMessageAsTransient() {
        Assertions.assertThat(TDengineRetryUtils.isTransient(new SQLException("network timeout")))
                .isTrue();
    }

    @Test
    void testSyntaxErrorIsNotTransient() {
        Assertions.assertThat(
                        TDengineRetryUtils.isTransient(new SQLException("syntax error", "42000")))
                .isFalse();
    }
}
