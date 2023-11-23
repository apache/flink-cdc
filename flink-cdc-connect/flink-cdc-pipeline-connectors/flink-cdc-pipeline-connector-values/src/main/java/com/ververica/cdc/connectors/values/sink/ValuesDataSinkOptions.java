/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.values.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.ConfigOptions;

/** Configurations for {@link ValuesDataSink}. */
public class ValuesDataSinkOptions {

    public static final ConfigOption<Boolean> MATERIALIZED_IN_MEMORY =
            ConfigOptions.key("materialized.in.memory")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "True if the DataChangeEvent need to be materialized in memory.");
}
