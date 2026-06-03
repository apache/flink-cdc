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

import org.apache.flink.cdc.connectors.tdengine.sink.TDengineClientWrapper;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineDataSinkConfig;
import org.apache.flink.cdc.connectors.tdengine.sink.TDengineTestUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/** Tests for {@link TDengineSchemaValidator}. */
class TDengineSchemaValidatorTest {

    @Test
    void testExpectedColumnsIncludeTimestampMetricsAndTags() {
        TDengineDataSinkConfig config = TDengineTestUtils.defaultConfig();

        Assertions.assertThat(
                        TDengineSchemaValidator.expectedColumns(TDengineTestUtils.SCHEMA, config))
                .containsEntry("ts", "TIMESTAMP")
                .containsEntry("temperature", "DOUBLE")
                .containsEntry("status", "NCHAR(256)")
                .containsEntry("location", "NCHAR(256)")
                .doesNotContainKey("device_id");
    }

    @Test
    void testValidateStableSchemaDetectsTypeMismatch() throws SQLException {
        List<TDengineColumnDescription> descriptions =
                Arrays.asList(
                        new TDengineColumnDescription("ts", "TIMESTAMP", false),
                        new TDengineColumnDescription("temperature", "FLOAT", false),
                        new TDengineColumnDescription("status", "NCHAR(256)", false),
                        new TDengineColumnDescription("location", "NCHAR(256)", true));
        TDengineClientWrapper client =
                new TDengineClientWrapper() {
                    @Override
                    public void execute(String sql) {}

                    @Override
                    public List<TDengineColumnDescription> describeStable(
                            TDengineTableInfo tableInfo) {
                        return descriptions;
                    }

                    @Override
                    public boolean isValid() {
                        return true;
                    }

                    @Override
                    public void close() {}
                };

        Assertions.assertThatThrownBy(
                        () ->
                                TDengineSchemaValidator.validateStableSchema(
                                        client,
                                        new TDengineTableInfo("test_db", "metrics"),
                                        TDengineTestUtils.SCHEMA,
                                        TDengineTestUtils.defaultConfig()))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("temperature");
    }
}
