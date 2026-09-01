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

package org.apache.flink.cdc.models.dummy;

import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;

/** Deterministic AI model client used by tests. */
public class DummyModelClient implements AiModelClient, SupportsTextGeneration, SupportsEmbedding {

    private static final long serialVersionUID = 1L;

    private final boolean debug;

    public DummyModelClient(boolean debug) {
        this.debug = debug;
    }

    @Override
    public String generate(String systemPrompt, String userInput) {
        if (debug) {
            System.out.printf("Received prompt: %s%nUser input: %s%n", systemPrompt, userInput);
        }
        if (systemPrompt.contains("\"category\"")) {
            return "{\"category\":\"dummy\",\"confidence\":1.0}";
        }
        if (systemPrompt.contains("\"translated_text\"")) {
            return "{\"translated_text\":\"dummy translation\",\"detected_language\":\"en\"}";
        }
        if (systemPrompt.contains("\"summary\"")) {
            return "{\"summary\":\"dummy summary\"}";
        }
        if (systemPrompt.contains("\"score\"")) {
            return "{\"score\":0.0,\"label\":\"neutral\",\"confidence\":1.0}";
        }
        if (systemPrompt.contains("\"extracted_json\"")) {
            return "{\"extracted_json\":{\"name\":\"dummy\"}}";
        }
        if (systemPrompt.contains("\"masked_text\"")) {
            return "{\"masked_text\":\"d***y\",\"detected_entities\":\"name\"}";
        }
        return "{\"result\":\"dummy response\"}";
    }

    @Override
    public float[] embed(String text) {
        return new float[] {3f, 1f, 4f, 1f, 5f, 9f, 2f, 6f};
    }

    @Override
    public void open() {
        if (debug) {
            System.out.println("Dummy model opened.");
        }
    }

    @Override
    public void close() {
        if (debug) {
            System.out.println("Dummy model closed.");
        }
    }
}
