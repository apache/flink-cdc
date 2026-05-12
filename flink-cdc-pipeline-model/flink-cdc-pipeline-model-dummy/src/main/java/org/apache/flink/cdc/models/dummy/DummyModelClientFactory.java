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

package org.apache.flink.cdc.models.dummy;

import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.AiModelClientFactory;
import org.apache.flink.cdc.common.model.ModelContext;

import java.util.Set;

/** SPI factory for {@link DummyModelClient}. For testing purposes only. */
public class DummyModelClientFactory implements AiModelClientFactory {

    @Override
    public String identifier() {
        return "dummy";
    }

    @Override
    public Set<String> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<String> optionalOptions() {
        return Set.of("debug");
    }

    @Override
    public AiModelClient createClient(ModelContext context) {
        boolean debug = Boolean.parseBoolean(context.getOptions().getOrDefault("debug", "false"));
        return new DummyModelClient(debug);
    }
}
