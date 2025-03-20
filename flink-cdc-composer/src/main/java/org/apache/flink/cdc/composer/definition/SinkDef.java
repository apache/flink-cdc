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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Definition of a data sink.
 *
 * <p>A sink definition contains:
 *
 * <ul>
 *   <li>type: connector type of the sink, which will be used for discovering sink implementation.
 *       Required in the definition.
 *   <li>name: name of the sink. Optional in the definition.
 *   <li>config: configuration of the sink
 * </ul>
 */
public class SinkDef {
    private final String type;
    @Nullable private final String name;
    private final Configuration config;
    private final Set<SchemaChangeEventType> includedSchemaEvolutionTypes;

    public SinkDef(String type, @Nullable String name, Configuration config) {
        this.type = type;
        this.name = name;
        this.config = config;
        this.includedSchemaEvolutionTypes =
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet());
    }

    public SinkDef(
            String type,
            @Nullable String name,
            Configuration config,
            Set<SchemaChangeEventType> includedSchemaEvolutionTypes) {
        this.type = type;
        this.name = name;
        this.config = config;
        this.includedSchemaEvolutionTypes = includedSchemaEvolutionTypes;
    }

    public String getType() {
        return type;
    }

    public Optional<String> getName() {
        return Optional.ofNullable(name);
    }

    public Configuration getConfig() {
        return config;
    }

    public Set<SchemaChangeEventType> getIncludedSchemaEvolutionTypes() {
        return includedSchemaEvolutionTypes;
    }

    @Override
    public String toString() {
        return "SinkDef{"
                + "type='"
                + type
                + '\''
                + ", name='"
                + name
                + '\''
                + ", config="
                + config
                + ", includedSchemaEvolutionTypes="
                + includedSchemaEvolutionTypes
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SinkDef sinkDef = (SinkDef) o;
        return Objects.equals(type, sinkDef.type)
                && Objects.equals(name, sinkDef.name)
                && Objects.equals(config, sinkDef.config)
                && Objects.equals(
                        includedSchemaEvolutionTypes, sinkDef.includedSchemaEvolutionTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, config, includedSchemaEvolutionTypes);
    }
}
