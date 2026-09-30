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

/** Built-in AI image function definitions. */
public enum AiImageFunctionDef {
    AI_IMAGE_COMPLETE(
            "AI_IMAGE_COMPLETE",
            RowType.of(
                    new DataType[] {DataTypes.BYTES(), DataTypes.STRING()},
                    new String[] {"image", "prompt"}),
            DataTypes.STRING(),
            Capability.IMAGE_TEXT_GENERATION),

    AI_IMAGE_EMBED(
            "AI_IMAGE_EMBED",
            RowType.of(new DataType[] {DataTypes.BYTES()}, new String[] {"image"}),
            DataTypes.ARRAY(DataTypes.FLOAT()),
            Capability.IMAGE_EMBEDDING);

    /** Capability required by an image AI function. */
    public enum Capability {
        IMAGE_TEXT_GENERATION,
        IMAGE_EMBEDDING
    }

    private final String functionName;
    private final RowType inputType;
    private final DataType outputType;
    private final Capability capability;

    AiImageFunctionDef(
            String functionName, RowType inputType, DataType outputType, Capability capability) {
        this.functionName = functionName;
        this.inputType = inputType;
        this.outputType = outputType;
        this.capability = capability;
    }

    public String getFunctionName() {
        return functionName;
    }

    /** Returns the parameter types after the model argument. */
    public RowType getInputType() {
        return inputType;
    }

    public DataType getOutputType() {
        return outputType;
    }

    public Capability getCapability() {
        return capability;
    }
}
