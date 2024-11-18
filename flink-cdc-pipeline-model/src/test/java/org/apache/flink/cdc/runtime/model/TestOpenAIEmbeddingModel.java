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
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** A test for {@link OpenAIEmbeddingModel}. */
class TestOpenAIEmbeddingModel {

    @Test
    @Disabled("For manual test as there is a limit for quota.")
    public void testEval() {
        OpenAIEmbeddingModel openAIEmbeddingModel = new OpenAIEmbeddingModel();
        Configuration configuration = new Configuration();
        configuration.set(ModelOptions.OPENAI_HOST, "http://langchain4j.dev/demo/openai/v1");
        configuration.set(ModelOptions.OPENAI_API_KEY, "demo");
        configuration.set(ModelOptions.OPENAI_MODEL_NAME, "text-embedding-3-small");
        UserDefinedFunctionContext userDefinedFunctionContext = () -> configuration;
        openAIEmbeddingModel.open(userDefinedFunctionContext);
        ArrayData arrayData =
                openAIEmbeddingModel.eval("Flink CDC is a streaming data integration tool");
        Assertions.assertThat(arrayData).isNotNull();
    }
}
