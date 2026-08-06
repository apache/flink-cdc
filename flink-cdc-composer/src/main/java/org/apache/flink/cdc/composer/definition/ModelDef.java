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

/** Common properties of model. */
public class ModelDef {

    private final String name;

    private final String type;

    private final Map<String, String> options;

    public ModelDef(String name, String type, Map<String, String> options) {
        this.name = name;
        this.type = type;
        this.options = options;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getOptions() {
        return options;
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
        return Objects.equals(name, modelDef.name)
                && Objects.equals(type, modelDef.type)
                && Objects.equals(options, modelDef.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, options);
    }

    @Override
    public String toString() {
        return "ModelDef{"
                + "name='"
                + name
                + '\''
                + ", type='"
                + type
                + '\''
                + ", options="
                + options
                + '}';
    }
}
