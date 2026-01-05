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

package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import org.apache.hudi.configuration.FlinkOptions;

/**
 * A utility class that holds all the configuration options for the Hudi sink. It wraps Hudi's
 * {@link FlinkOptions} to provide a consistent interface within the CDC framework, using helper
 * methods to reduce boilerplate.
 */
public class HudiConfig {

    // ----- Helper Methods for Option Creation -----

    private static ConfigOption<String> stringOption(String key, Description description) {
        return ConfigOptions.key(key)
                .stringType()
                .noDefaultValue()
                .withDescription(description.toString());
    }

    private static ConfigOption<String> stringOption(
            String key, String defaultValue, Description description) {
        return ConfigOptions.key(key)
                .stringType()
                .defaultValue(defaultValue)
                .withDescription(description.toString());
    }

    private static ConfigOption<Integer> intOption(
            String key, int defaultValue, Description description) {
        return ConfigOptions.key(key)
                .intType()
                .defaultValue(defaultValue)
                .withDescription(description.toString());
    }

    private static ConfigOption<Boolean> booleanOption(
            String key, boolean defaultValue, Description description) {
        return ConfigOptions.key(key)
                .booleanType()
                .defaultValue(defaultValue)
                .withDescription(description.toString());
    }

    // ----- Public Configuration Options -----

    // Core Hudi Options
    public static final ConfigOption<String> PATH =
            stringOption(FlinkOptions.PATH.key(), FlinkOptions.PATH.description());

    public static final ConfigOption<String> TABLE_TYPE =
            stringOption(
                    "hoodie.table.type",
                    FlinkOptions.TABLE_TYPE.defaultValue(),
                    FlinkOptions.TABLE_TYPE.description());

    public static final ConfigOption<String> ORDERING_FIELDS =
            stringOption(
                    FlinkOptions.ORDERING_FIELDS.key(), FlinkOptions.ORDERING_FIELDS.description());

    // Bucket Index Options
    public static final ConfigOption<String> INDEX_TYPE =
            stringOption(
                    FlinkOptions.INDEX_TYPE.key(), "BUCKET", FlinkOptions.INDEX_TYPE.description());

    public static final ConfigOption<Integer> WRITE_TASKS =
            intOption(FlinkOptions.WRITE_TASKS.key(), 4, FlinkOptions.WRITE_TASKS.description());
}
