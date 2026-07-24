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

package org.apache.flink.cdc.models.openai;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiCompatibleModelClientTest {

    @Test
    void testMergeSystemPromptWithGlobalAndRuntime() {
        assertThat(OpenAiCompatibleModelClient.mergeSystemPrompt("global", "runtime"))
                .isEqualTo("global\n\nruntime");
    }

    @Test
    void testMergeSystemPromptWithGlobalOnly() {
        assertThat(OpenAiCompatibleModelClient.mergeSystemPrompt("global", null))
                .isEqualTo("global");
    }

    @Test
    void testMergeSystemPromptWithRuntimeOnly() {
        assertThat(OpenAiCompatibleModelClient.mergeSystemPrompt(null, "runtime"))
                .isEqualTo("runtime");
    }

    @Test
    void testMergeSystemPromptSkipsBlankGlobal() {
        assertThat(OpenAiCompatibleModelClient.mergeSystemPrompt("   ", "runtime"))
                .isEqualTo("runtime");
    }

    @Test
    void testMergeSystemPromptSkipsBlankRuntime() {
        assertThat(OpenAiCompatibleModelClient.mergeSystemPrompt("global", "   "))
                .isEqualTo("global");
    }

    @Test
    void testMergeSystemPromptReturnsEmptyWhenBothBlank() {
        assertThat(OpenAiCompatibleModelClient.mergeSystemPrompt(" ", " ")).isEmpty();
    }
}
