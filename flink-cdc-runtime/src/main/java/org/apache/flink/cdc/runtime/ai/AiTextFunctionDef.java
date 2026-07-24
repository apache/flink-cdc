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

package org.apache.flink.cdc.runtime.ai;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;

/**
 * Built-in AI text generation function definitions with their prompt templates and type metadata.
 */
public enum AiTextFunctionDef {
    AI_COMPLETE(
            "AI_COMPLETE",
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"systemPrompt"}),
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"result"}),
            "%s\n"),

    AI_SUMMARIZE(
            "AI_SUMMARIZE",
            RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"maxLength"}),
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"summary"}),
            "You are a text summarization expert. Generate an accurate, coherent, and informative "
                    + "summary that does not exceed %d characters.\n"
                    + "Output requirements:\n"
                    + "- summary: the summarized content\n"
                    + "Principles:\n"
                    + "- Stay within the specified length\n"
                    + "- Preserve core ideas and key information\n"
                    + "- Use concise language with clear logic\n"
                    + "- Maintain text coherence\n"
                    + "- Avoid subjective opinions\n");

    private final String functionName;
    private final RowType inputType;
    private final RowType outputType;
    private final String promptTemplate;

    AiTextFunctionDef(
            String functionName, RowType inputType, RowType outputType, String promptTemplate) {
        this.functionName = functionName;
        this.inputType = inputType;
        this.outputType = outputType;
        this.promptTemplate = promptTemplate;
    }

    public String getFunctionName() {
        return functionName;
    }

    /**
     * Returns the additional parameter types for promptTemplate placeholders.
     *
     * <p>Input text parameter is always added by runtime, not included here.
     */
    public RowType getInputType() {
        return inputType;
    }

    public RowType getOutputType() {
        return outputType;
    }

    /** Builds the core system prompt by filling in the template placeholders. */
    public String buildPrompt(Object... args) {
        return String.format(promptTemplate, args);
    }
}
