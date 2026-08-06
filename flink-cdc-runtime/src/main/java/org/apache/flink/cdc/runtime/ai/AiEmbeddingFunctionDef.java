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

/** Built-in AI embedding function definitions with configurable input and output types. */
public enum AiEmbeddingFunctionDef {
    AI_EMBED("AI_EMBED", DataTypes.STRING(), DataTypes.ARRAY(DataTypes.FLOAT()));

    private final String functionName;
    private final DataType inputType;
    private final DataType outputType;

    AiEmbeddingFunctionDef(String functionName, DataType inputType, DataType outputType) {
        this.functionName = functionName;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public String getFunctionName() {
        return functionName;
    }

    /** The type of the input value (e.g. STRING for text embedding). */
    public DataType getInputType() {
        return inputType;
    }

    /** The type of the output value (e.g. ARRAY&lt;FLOAT&gt; for vector embedding). */
    public DataType getOutputType() {
        return outputType;
    }
}
