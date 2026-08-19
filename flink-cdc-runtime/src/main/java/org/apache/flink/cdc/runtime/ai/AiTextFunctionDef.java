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

import java.util.Locale;

/** Built-in AI text generation function definitions. */
public enum AiTextFunctionDef {
    AI_COMPLETE(
            "AI_COMPLETE",
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"systemPrompt"}),
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"result"}),
            "%s\n"),

    AI_CLASSIFY(
            "AI_CLASSIFY",
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"labels"}),
            RowType.of(
                    new DataType[] {DataTypes.STRING(), DataTypes.DOUBLE()},
                    new String[] {"category", "confidence"}),
            "You are a text classifier. Classify the input into exactly one of these labels: %s.\n"
                    + "Choose only a provided label. Use the dominant meaning when multiple labels apply, "
                    + "and lower the confidence when no label is a good match.\n"),

    AI_TRANSLATE(
            "AI_TRANSLATE",
            RowType.of(
                    new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                    new String[] {"sourceLang", "targetLang"}),
            RowType.of(
                    new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                    new String[] {"translated_text", "detected_language"}),
            "You are a translator. Translate the input from %s to %s while preserving its meaning, "
                    + "formatting, and terminology. If the source language is auto, detect it and report "
                    + "the detected language code.\n"),

    AI_SUMMARIZE(
            "AI_SUMMARIZE",
            RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"maxLength"}),
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"summary"}),
            "You are a text summarizer. Summarize the input in no more than %d characters. Preserve "
                    + "the key facts and conclusions, remove redundancy, and avoid subjective commentary.\n"),

    AI_SENTIMENT(
            "AI_SENTIMENT",
            RowType.of(new DataType[0], new String[0]),
            RowType.of(
                    new DataType[] {DataTypes.DOUBLE(), DataTypes.STRING(), DataTypes.DOUBLE()},
                    new String[] {"score", "label", "confidence"}),
            "You are a sentiment analyzer. Analyze the input in context. Return a score from -1.0 "
                    + "(most negative) to 1.0 (most positive), a label of positive, negative, or neutral, "
                    + "and a confidence from 0.0 to 1.0. Consider tone, negation, and sarcasm.\n"),

    AI_EXTRACT(
            "AI_EXTRACT",
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"schema"}),
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"extracted_json"}),
            "You are an information extraction system. Extract information from the input according "
                    + "to this schema: %s. Preserve the requested field names and types, use null for "
                    + "missing values, and place the extracted JSON object in extracted_json. Supported "
                    + "types include string, number, integer, boolean, array, object, date, datetime, "
                    + "email, and phone.\n"),

    AI_MASK(
            "AI_MASK",
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"entities"}),
            RowType.of(
                    new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                    new String[] {"masked_text", "detected_entities"}),
            "You are a data masking system. Detect and consistently mask these entity types in the "
                    + "input: %s. Preserve the usefulness and structure of non-sensitive content, and "
                    + "report the entities that were detected.\n");

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

    /** Returns the parameter types after the model and input arguments. */
    public RowType getInputType() {
        return inputType;
    }

    public RowType getOutputType() {
        return outputType;
    }

    public String buildPrompt(Object... args) {
        return String.format(Locale.ROOT, promptTemplate, args);
    }
}
