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

package org.apache.flink.cdc.composer.definition;

import java.util.Map;
import java.util.Objects;

/**
 * Common properties of model.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>modelName: The name of function.
 *   <li>className: The model to transform data.
 *   <li>parameters: The parameters that used to configure the model.
 * </ul>
 */
public class ModelDef {

    private final String modelName;

    private final String className;

    private final Map<String, String> parameters;

    public ModelDef(String modelName, String className, Map<String, String> parameters) {
        this.modelName = modelName;
        this.className = className;
        this.parameters = parameters;
    }

    public String getModelName() {
        return modelName;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ModelDef modelDef = (ModelDef) o;
        return Objects.equals(modelName, modelDef.modelName)
                && Objects.equals(className, modelDef.className)
                && Objects.equals(parameters, modelDef.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelName, className, parameters);
    }

    @Override
    public String toString() {
        return "ModelDef{"
                + "name='"
                + modelName
                + '\''
                + ", model='"
                + className
                + '\''
                + ", parameters="
                + parameters
                + '}';
    }
}
