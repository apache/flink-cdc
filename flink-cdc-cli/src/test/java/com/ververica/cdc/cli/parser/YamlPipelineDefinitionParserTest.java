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

import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava30.com.google.common.io.Resources;

import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.RouteDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

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
        ImmutableMap.<String, String>builder().put("parallelism", "1").put("foo", "bar");
        PipelineDef pipelineDef = parser.parse(Paths.get(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDefWithGlobalConf);
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
                                    Pattern.compile("mydb.default.app_order_.*"),
                                    "odsdb.default.app_order",
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    Pattern.compile("mydb.default.web_order"),
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
                                    Pattern.compile("mydb.default.app_order_.*"),
                                    "odsdb.default.app_order",
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    Pattern.compile("mydb.default.web_order"),
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
                                    Pattern.compile("mydb.default.app_order_.*"),
                                    "odsdb.default.app_order",
                                    null)),
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
