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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.runtime.model.OpenAIEmbeddingModel;
import org.apache.flink.table.functions.ScalarFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testcases for {@link UserDefinedFunctionDescriptor}. */
class UserDefinedFunctionDescriptorTest {

    /** This is a plain Flink CDC UDF. */
    public static class CdcUdf implements UserDefinedFunction {}

    /** This is a Flink CDC UDF with type hint. */
    public static class CdcUdfWithTypeHint implements UserDefinedFunction {
        @Override
        public DataType getReturnType() {
            return DataTypes.TIMESTAMP_LTZ(9);
        }
    }

    /** This is a Flink ScalarFunction. */
    public static class FlinkUdf extends ScalarFunction {}

    /** This is not a valid UDF class. */
    public static class NotUDF {}

    @Test
    void testUserDefinedFunctionDescriptor() throws JsonProcessingException {

        assertThat(new UserDefinedFunctionDescriptor("cdc_udf", CdcUdf.class.getName()))
                .extracting("name", "className", "classpath", "returnTypeHint", "isCdcPipelineUdf")
                .containsExactly(
                        "cdc_udf",
                        "UserDefinedFunctionDescriptorTest$CdcUdf",
                        "org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptorTest$CdcUdf",
                        null,
                        true);

        assertThat(
                        new UserDefinedFunctionDescriptor(
                                "cdc_udf_with_type_hint", CdcUdfWithTypeHint.class.getName()))
                .extracting("name", "className", "classpath", "returnTypeHint", "isCdcPipelineUdf")
                .containsExactly(
                        "cdc_udf_with_type_hint",
                        "UserDefinedFunctionDescriptorTest$CdcUdfWithTypeHint",
                        "org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptorTest$CdcUdfWithTypeHint",
                        DataTypes.TIMESTAMP_LTZ(9),
                        true);

        assertThat(new UserDefinedFunctionDescriptor("flink_udf", FlinkUdf.class.getName()))
                .extracting("name", "className", "classpath", "returnTypeHint", "isCdcPipelineUdf")
                .containsExactly(
                        "flink_udf",
                        "UserDefinedFunctionDescriptorTest$FlinkUdf",
                        "org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptorTest$FlinkUdf",
                        null,
                        false);

        assertThatThrownBy(
                        () -> new UserDefinedFunctionDescriptor("not_udf", NotUDF.class.getName()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Failed to detect UDF class class org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptorTest$NotUDF "
                                + "since it never implements interface org.apache.flink.cdc.common.udf.UserDefinedFunction or "
                                + "extends Flink class org.apache.flink.table.functions.ScalarFunction.");

        assertThatThrownBy(
                        () ->
                                new UserDefinedFunctionDescriptor(
                                        "not_even_exist", "not.a.valid.class.path"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to instantiate UDF not_even_exist@not.a.valid.class.path");
        String name = "GET_EMBEDDING";
        assertThat(new UserDefinedFunctionDescriptor(name, OpenAIEmbeddingModel.class.getName()))
                .extracting("name", "className", "classpath", "returnTypeHint", "isCdcPipelineUdf")
                .containsExactly(
                        "GET_EMBEDDING",
                        "OpenAIEmbeddingModel",
                        "org.apache.flink.cdc.runtime.model.OpenAIEmbeddingModel",
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        true);
    }
}
