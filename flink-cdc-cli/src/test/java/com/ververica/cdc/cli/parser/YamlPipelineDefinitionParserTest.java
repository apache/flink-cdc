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

package com.ververica.cdc.cli.parser;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static com.ververica.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link YamlPipelineDefinitionParser}. */
class YamlPipelineDefinitionParserTest {

    @Test
    void testParsingFullDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDef);
    }

    @Test
    void testParsingNecessaryOnlyDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-with-optional.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(defWithOptional);
    }

    @Test
    void testMinimizedDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(minimizedDef);
    }

    @Test
    void testOverridingGlobalConfig() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("parallelism", "1")
                                        .put("foo", "bar")
                                        .build()));
        assertThat(pipelineDef).isEqualTo(fullDefWithGlobalConf);
    }

    @Test
    void testEvaluateDefaultLocalTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE))
                .isNotEqualTo(PIPELINE_LOCAL_TIME_ZONE.defaultValue());
    }

    @Test
    void testValidTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "Asia/Shanghai")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE))
                .isEqualTo("Asia/Shanghai");

        pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "GMT+08:00")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE)).isEqualTo("GMT+08:00");

        pipelineDef =
                parser.parse(
                        Paths.get(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "UTC")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE)).isEqualTo("UTC");
    }

    @Test
    void testInvalidTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        assertThatThrownBy(
                        () ->
                                parser.parse(
                                        Paths.get(resource.toURI()),
                                        Configuration.fromMap(
                                                ImmutableMap.<String, String>builder()
                                                        .put(
                                                                PIPELINE_LOCAL_TIME_ZONE.key(),
                                                                "invalid time zone")
                                                        .build())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Invalid time zone. The valid value should be a Time Zone Database ID"
                                + " such as 'America/Los_Angeles' to include daylight saving time. "
                                + "Fixed offsets are supported using 'GMT-08:00' or 'GMT+08:00'. "
                                + "Or use 'UTC' without time zone and daylight saving time.");
    }

    private final PipelineDef fullDef =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .put(
                                                    "chunk-column",
                                                    "app_order_.*:id,web_order:product_id")
                                            .put("capture-new-tables", "true")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .put("auto-create-table", "true")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order",
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order",
                                    "sync table to with given prefix ods_")),
                    null,
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("enable-schema-evolution", "false")
                                    .build()));

    private final PipelineDef fullDefWithGlobalConf =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            "source-database",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .put(
                                                    "chunk-column",
                                                    "app_order_.*:id,web_order:product_id")
                                            .put("capture-new-tables", "true")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            "sink-queue",
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .put("auto-create-table", "true")
                                            .build())),
                    Arrays.asList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order",
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order",
                                    "sync table to with given prefix ods_")),
                    null,
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("enable-schema-evolution", "false")
                                    .put("foo", "bar")
                                    .build()));

    private final PipelineDef defWithOptional =
            new PipelineDef(
                    new SourceDef(
                            "mysql",
                            null,
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("host", "localhost")
                                            .put("port", "3306")
                                            .put("username", "admin")
                                            .put("password", "pass")
                                            .put(
                                                    "tables",
                                                    "adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*")
                                            .build())),
                    new SinkDef(
                            "kafka",
                            null,
                            Configuration.fromMap(
                                    ImmutableMap.<String, String>builder()
                                            .put("bootstrap-servers", "localhost:9092")
                                            .build())),
                    Collections.singletonList(
                            new RouteDef(
                                    "mydb.default.app_order_.*", "odsdb.default.app_order", null)),
                    null,
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("parallelism", "4")
                                    .build()));

    private final PipelineDef minimizedDef =
            new PipelineDef(
                    new SourceDef("mysql", null, new Configuration()),
                    new SinkDef("kafka", null, new Configuration()),
                    Collections.emptyList(),
                    null,
                    new Configuration());
}
