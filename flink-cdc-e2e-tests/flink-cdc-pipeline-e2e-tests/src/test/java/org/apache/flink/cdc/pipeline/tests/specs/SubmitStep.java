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

package org.apache.flink.cdc.pipeline.tests.specs;

import java.util.List;
import java.util.stream.Collectors;

/** A {@link SpecStep} to submit YAML jobs. */
class SubmitStep implements SpecStep {
    public String yaml;
    public List<String> expectError;

    public SubmitStep(String yaml, List<String> expectError) {
        this.yaml = yaml;
        this.expectError = expectError;
    }

    public SubmitStep substitute(String key, String value) {
        this.yaml = this.yaml.replace(key, value);
        this.expectError =
                this.expectError.stream()
                        .map(l -> l.replaceAll(key, value))
                        .collect(Collectors.toList());

        return this;
    }
}
