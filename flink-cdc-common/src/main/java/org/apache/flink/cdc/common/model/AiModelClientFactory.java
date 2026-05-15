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

package org.apache.flink.cdc.common.model;

import org.apache.flink.cdc.common.annotation.Experimental;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SPI interface for AI model client factories. Each provider (e.g. OpenAI-compatible, DashScope)
 * ships one implementation, discoverable via {@link java.util.ServiceLoader}.
 *
 * <p>The {@link #identifier()} value maps to the {@code type} field of a {@code pipeline.model}
 * entry in the pipeline YAML.
 */
@Experimental
public interface AiModelClientFactory {

    /** A unique, lower-case identifier for this provider, e.g. {@code "openai-compatible"}. */
    String identifier();

    /** Option keys that must be present in the model YAML options block. */
    Set<String> requiredOptions();

    /** Option keys that may optionally appear in the model YAML options block. */
    Set<String> optionalOptions();

    /**
     * Validates that the given context contains all required options and no unknown options.
     * Subclasses may override this to add custom validation logic.
     */
    default void validate(ModelContext context) {
        Set<String> required = requiredOptions();
        Set<String> optional = optionalOptions();
        if (required != null) {
            Set<String> missing =
                    required.stream()
                            .filter(k -> !context.getOptions().containsKey(k))
                            .collect(Collectors.toSet());
            if (!missing.isEmpty()) {
                throw new IllegalArgumentException(
                        "Missing required options for model '"
                                + context.getModelName()
                                + "' (type='"
                                + identifier()
                                + "'): "
                                + missing);
            }
        }
        if (required != null && optional != null) {
            List<String> unknown =
                    context.getOptions().keySet().stream()
                            .filter(k -> !required.contains(k) && !optional.contains(k))
                            .sorted()
                            .collect(Collectors.toList());
            if (!unknown.isEmpty()) {
                throw new IllegalArgumentException(
                        "Unknown options for model '"
                                + context.getModelName()
                                + "' (type='"
                                + identifier()
                                + "'): "
                                + unknown);
            }
        }
    }

    /**
     * Creates a new {@link AiModelClient} from the given context. Called once per model definition
     * at pipeline assembly time on the job-manager side; the returned client is serialized and
     * shipped to task managers.
     */
    AiModelClient createClient(ModelContext context);
}
