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

package org.apache.flink.cdc.common.pipeline;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.description.Description;
import org.apache.flink.cdc.common.configuration.description.ListElement;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.cdc.common.configuration.description.TextElement.text;

/** Predefined pipeline configuration options. */
@PublicEvolving
public class PipelineOptions {

    public static final Duration DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT = Duration.ofMinutes(3);

    public static final ConfigOption<String> PIPELINE_NAME =
            ConfigOptions.key("name")
                    .stringType()
                    .defaultValue("Flink CDC Pipeline Job")
                    .withDescription("The name of the pipeline");

    public static final ConfigOption<Integer> PIPELINE_PARALLELISM =
            ConfigOptions.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Parallelism of the pipeline");

    public static final ConfigOption<RuntimeExecutionMode> PIPELINE_EXECUTION_RUNTIME_MODE =
            ConfigOptions.key("execution.runtime-mode")
                    .enumType(RuntimeExecutionMode.class)
                    .defaultValue(RuntimeExecutionMode.STREAMING)
                    .withDescription("Runtime execution mode of the pipeline");

    public static final ConfigOption<SchemaChangeBehavior> PIPELINE_SCHEMA_CHANGE_BEHAVIOR =
            ConfigOptions.key("schema.change.behavior")
                    .enumType(SchemaChangeBehavior.class)
                    .defaultValue(SchemaChangeBehavior.LENIENT)
                    .withDescription(
                            Description.builder()
                                    .text("Behavior for handling schema change events. ")
                                    .linebreak()
                                    .add(
                                            ListElement.list(
                                                    text("IGNORE: Drop all schema change events."),
                                                    text(
                                                            "LENIENT: Apply schema changes to downstream tolerantly, and keeps executing if applying fails."),
                                                    text(
                                                            "TRY_EVOLVE: Apply schema changes to downstream, but keeps executing if applying fails."),
                                                    text(
                                                            "EVOLVE: Apply schema changes to downstream. This requires sink to support handling schema changes."),
                                                    text(
                                                            "EXCEPTION: Throw an exception to terminate the sync pipeline.")))
                                    .build());

    public static final ConfigOption<String> PIPELINE_LOCAL_TIME_ZONE =
            ConfigOptions.key("local-time-zone")
                    .stringType()
                    // "systemDefault" is a special value to decide whether to use
                    // ZoneId.systemDefault() in
                    // PipelineOptions.getLocalTimeZone()
                    .defaultValue("systemDefault")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The local time zone defines current session time zone id. ")
                                    .linebreak()
                                    .text(
                                            "It is used when converting to/from <code>TIMESTAMP WITH LOCAL TIME ZONE</code>. "
                                                    + "Internally, timestamps with local time zone are always represented in the UTC time zone. "
                                                    + "However, when converting to data types that don't include a time zone (e.g. TIMESTAMP, STRING), "
                                                    + "the session time zone is used during conversion. The input of option is either a full name "
                                                    + "such as \"America/Los_Angeles\", or a custom timezone id such as \"GMT-08:00\".")
                                    .build());

    public static final ConfigOption<String> PIPELINE_OPERATOR_UID_PREFIX =
            ConfigOptions.key("operator.uid.prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The prefix to use for all pipeline operator UIDs. If not set, all pipeline operator UIDs"
                                    + " will be generated by Flink. It is recommended to set this parameter to ensure"
                                    + " stable and recognizable operator UIDs, which can help with stateful upgrades,"
                                    + " troubleshooting, and Flink UI diagnostics.");

    @Deprecated
    public static final ConfigOption<String> PIPELINE_SCHEMA_OPERATOR_UID =
            ConfigOptions.key("schema.operator.uid")
                    .stringType()
                    .defaultValue("$$_schema_operator_$$")
                    .withDescription(
                            "The unique ID for schema operator. This ID will be used for inter-operator communications "
                                    + "and must be unique across operators. Deprecated, use "
                                    + PIPELINE_OPERATOR_UID_PREFIX.key()
                                    + " instead.");

    public static final ConfigOption<Duration> PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT =
            ConfigOptions.key("schema-operator.rpc-timeout")
                    .durationType()
                    .defaultValue(DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT)
                    .withDescription(
                            "The timeout time for SchemaOperator to wait downstream SchemaChangeEvent applying finished, the default value is 3 minutes.");

    public static final ConfigOption<String> SHADE_IDENTIFIER_OPTION =
            ConfigOptions.key("shade.identifier")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "The identifier of the encryption method for decryption. Defaults to \"default\", indicating no encryption");

    public static final ConfigOption<List<String>> SHADE_SENSITIVE_KEYWORDS =
            ConfigOptions.key("shade.sensitive.keywords")
                    .stringType()
                    .asList()
                    .defaultValues("password")
                    .withDescription(
                            "A semicolon-separated list of keywords of the configuration items to be decrypted.");

    private PipelineOptions() {}
}
