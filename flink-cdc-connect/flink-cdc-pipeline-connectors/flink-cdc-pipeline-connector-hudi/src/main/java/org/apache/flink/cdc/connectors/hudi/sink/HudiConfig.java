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

import org.apache.hudi.common.config.HoodieCommonConfig;
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

    private static ConfigOption<Integer> intOption(String key, Description description) {
        return ConfigOptions.key(key)
                .intType()
                .noDefaultValue()
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

    //    public static final ConfigOption<String> TABLE_TYPE =
    //            stringOption(
    //                    FlinkOptions.TABLE_TYPE.key(),
    //                    FlinkOptions.TABLE_TYPE.defaultValue(),
    //                    FlinkOptions.TABLE_TYPE.description());
    public static final ConfigOption<String> TABLE_TYPE =
            stringOption(
                    "hoodie.table.type",
                    FlinkOptions.TABLE_TYPE.defaultValue(),
                    FlinkOptions.TABLE_TYPE.description());

    // Required Fields for CDC
    public static final ConfigOption<String> RECORD_KEY_FIELD =
            stringOption(
                    FlinkOptions.RECORD_KEY_FIELD.key(),
                    FlinkOptions.RECORD_KEY_FIELD.description());

    public static final ConfigOption<String> ORDERING_FIELDS =
            stringOption(
                    FlinkOptions.ORDERING_FIELDS.key(), FlinkOptions.ORDERING_FIELDS.description());

    public static final ConfigOption<String> PARTITION_PATH_FIELD =
            stringOption(
                    FlinkOptions.PARTITION_PATH_FIELD.key(),
                    "",
                    FlinkOptions.PARTITION_PATH_FIELD.description());

    // Bucket Index Options
    public static final ConfigOption<String> INDEX_TYPE =
            stringOption(
                    FlinkOptions.INDEX_TYPE.key(), "BUCKET", FlinkOptions.INDEX_TYPE.description());

    public static final ConfigOption<String> INDEX_BUCKET_TARGET =
            stringOption(
                    FlinkOptions.INDEX_KEY_FIELD.key(), FlinkOptions.INDEX_KEY_FIELD.description());

    public static final ConfigOption<Integer> BUCKET_INDEX_NUM_BUCKETS =
            intOption(
                    FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(),
                    FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.description());

    // Hive Sync Options
    public static final ConfigOption<Boolean> HIVE_SYNC_ENABLED =
            booleanOption(
                    FlinkOptions.HIVE_SYNC_ENABLED.key(),
                    false,
                    FlinkOptions.HIVE_SYNC_ENABLED.description());

    public static final ConfigOption<String> HIVE_SYNC_METASTORE_URIS =
            stringOption(
                    FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(),
                    FlinkOptions.HIVE_SYNC_METASTORE_URIS.description());

    public static final ConfigOption<String> HIVE_SYNC_DB =
            stringOption(FlinkOptions.HIVE_SYNC_DB.key(), FlinkOptions.HIVE_SYNC_DB.description());

    public static final ConfigOption<String> HIVE_SYNC_TABLE =
            stringOption(
                    FlinkOptions.HIVE_SYNC_TABLE.key(), FlinkOptions.HIVE_SYNC_TABLE.description());

    public static final ConfigOption<String> SCHEMA_OPERATOR_UID =
            ConfigOptions.key("schema.operator.uid")
                    .stringType()
                    .defaultValue("schema-operator-uid")
                    .withDescription(
                            "A unique ID for the schema operator, used by the BucketAssignerOperator to create a SchemaEvolutionClient.");

    public static final ConfigOption<String> TABLE_SCHEMA =
            ConfigOptions.key("table.schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table schema in JSON format for the Hudi table.");

    public static final ConfigOption<Integer> BUCKET_ASSIGN_TASKS =
            intOption(
                    FlinkOptions.BUCKET_ASSIGN_TASKS.key(),
                    FlinkOptions.BUCKET_ASSIGN_TASKS.description());

    public static final ConfigOption<Integer> WRITE_TASKS =
            intOption(FlinkOptions.WRITE_TASKS.key(), FlinkOptions.WRITE_TASKS.description());

    public static final ConfigOption<Boolean> SCHEMA_ON_READ_ENABLE =
            booleanOption(
                    HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(),
                    false,
                    Description.builder().build());

    public static final ConfigOption<Integer> COMPACTION_DELTA_COMMITS =
            ConfigOptions.key("compaction.delta_commits")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Max delta commits needed to trigger compaction, default 5 commits");
}
