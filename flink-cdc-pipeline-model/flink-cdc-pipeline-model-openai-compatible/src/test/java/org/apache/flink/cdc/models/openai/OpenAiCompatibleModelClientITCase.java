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

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiCompatibleModelClientITCase {

    private OpenAiCompatibleModelClient client;

    @BeforeEach
    void setUp() {
        String endpoint = System.getenv("OPENAI_BASE_URL");
        String apiKey = System.getenv("OPENAI_API_KEY");
        String model = System.getenv("OPENAI_MODEL");
        Assumptions.assumeThat(endpoint != null && apiKey != null && model != null)
                .as("OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL must be set")
                .isTrue();

        client = new OpenAiCompatibleModelClient(endpoint, apiKey, model);
        client.open();
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testGenerate() {
        String result =
                client.generate("You are a calculator.", "What is 1 + 1? Answer only the number.");
        assertThat(result).isNotNull().contains("2");
    }

    @Test
    void testGenerateWithEmptyUserInput() {
        String result = client.generate("Reply with exactly: OK", "");
        assertThat(result).isNotNull().contains("OK");
    }
}
