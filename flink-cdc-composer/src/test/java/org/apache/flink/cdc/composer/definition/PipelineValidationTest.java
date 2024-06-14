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
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Tests for {@link PipelineDef} validation. */
public class PipelineValidationTest {

    @Test
    void testNormalConfigValidation() {
        // A common configuration file
        Map<String, String> configurations = new HashMap<>();

        configurations.put("parallelism", "1");
        configurations.put("name", "Pipeline Job");

        PipelineDef.validatePipelineDefinition(Configuration.fromMap(configurations));
    }

    @Test
    void testTypeMismatchValidation() {
        Map<String, String> configurations = new HashMap<>();

        // option value with mismatched type.
        configurations.put("parallelism", "Not a Number");
        configurations.put("name", "Pipeline Job");

        Assertions.assertThrowsExactly(
                IllegalArgumentException.class,
                () -> PipelineDef.validatePipelineDefinition(Configuration.fromMap(configurations)),
                "Could not parse value 'Not a Number' for key 'parallelism'.");
    }

    @Test
    void testEmptyConfigValidation() {

        // An empty configuration should fail
        Map<String, String> configurations = new HashMap<>();

        Assertions.assertThrowsExactly(
                ValidationException.class,
                () ->
                        PipelineDef.validatePipelineDefinition(
                                Configuration.fromMap(configurations)));
    }

    @Test
    void testUnknownConfigValidation() {
        // An empty configuration should fail
        Map<String, String> configurations = new HashMap<>();

        configurations.put("parallelism", "1");
        configurations.put("name", "Pipeline Job");
        configurations.put("unknown", "optionValue");

        Assertions.assertThrowsExactly(
                ValidationException.class,
                () ->
                        PipelineDef.validatePipelineDefinition(
                                Configuration.fromMap(configurations)));
    }
}
