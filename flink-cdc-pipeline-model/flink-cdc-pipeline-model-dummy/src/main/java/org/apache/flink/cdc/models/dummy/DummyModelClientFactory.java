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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.AiModelClientFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.model.AiModelClient;

import java.util.Set;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** SPI factory for {@link DummyModelClient}. For testing purposes only. */
public class DummyModelClientFactory implements AiModelClientFactory {

    public static final ConfigOption<Boolean> DEBUG =
            key("debug").booleanType().defaultValue(false);

    @Override
    public String identifier() {
        return "dummy";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(DEBUG);
    }

    @Override
    public AiModelClient createClient(Factory.Context context) {
        boolean debug = context.getFactoryConfiguration().get(DEBUG);
        return new DummyModelClient(debug);
    }
}
