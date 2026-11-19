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

package org.apache.flink.cdc.runtime.model;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** A test for {@link OpenAIChatModel}. */
class TestOpenAIChatModel {
    @Test
    @Disabled("For manual test as there is a limit for quota.")
    public void testEval() {
        OpenAIChatModel openAIChatModel = new OpenAIChatModel();
        Configuration configuration = new Configuration();
        configuration.set(ModelOptions.OPENAI_HOST, "http://langchain4j.dev/demo/openai/v1");
        configuration.set(ModelOptions.OPENAI_API_KEY, "demo");
        configuration.set(ModelOptions.OPENAI_MODEL_NAME, "gpt-4o-mini");
        UserDefinedFunctionContext userDefinedFunctionContext = () -> configuration;
        openAIChatModel.open(userDefinedFunctionContext);
        String response = openAIChatModel.eval("Who invented the electric light?");
        Assertions.assertThat(response).isNotEmpty();
    }
}
