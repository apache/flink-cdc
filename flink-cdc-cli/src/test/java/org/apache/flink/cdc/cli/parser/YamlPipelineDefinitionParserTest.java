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

package org.apache.flink.cdc.cli.parser;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_COLUMN_TYPE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.DROP_TABLE;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.RENAME_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.TRUNCATE_TABLE;
import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser}. */
class YamlPipelineDefinitionParserTest {

    @Test
    void testParsingFullDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(new Path(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDef);
    }

    @Test
    void testParsingNecessaryOnlyDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-with-optional.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(new Path(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(defWithOptional);
    }

    @Test
    void testMinimizedDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(new Path(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(minimizedDef);
    }

    @Test
    void testOverridingGlobalConfig() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-full.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        new Path(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("parallelism", "1")
                                        .build()));
        assertThat(pipelineDef).isEqualTo(fullDefWithGlobalConf);
    }

    @Test
    void testEvaluateDefaultLocalTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(new Path(resource.toURI()), new Configuration());
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE))
                .isNotEqualTo(PIPELINE_LOCAL_TIME_ZONE.defaultValue());
    }

    @Test
    void testEvaluateDefaultRpcTimeOut() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        new Path(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(
                                                PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT
                                                        .key(),
                                                "1h")
                                        .build()));
        assertThat(
                        pipelineDef
                                .getConfig()
                                .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(60 * 60));
    }

    @Test
    void testValidTimeZone() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        new Path(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "Asia/Shanghai")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE))
                .isEqualTo("Asia/Shanghai");

        pipelineDef =
                parser.parse(
                        new Path(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "GMT+08:00")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE)).isEqualTo("GMT+08:00");

        pipelineDef =
                parser.parse(
                        new Path(resource.toURI()),
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put(PIPELINE_LOCAL_TIME_ZONE.key(), "UTC")
                                        .build()));
        assertThat(pipelineDef.getConfig().get(PIPELINE_LOCAL_TIME_ZONE)).isEqualTo("UTC");
    }

    @Test
    void testInvalidTimeZone() {
        URL resource = Resources.getResource("definitions/pipeline-definition-minimized.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        assertThatThrownBy(
                        () ->
                                parser.parse(
                                        new Path(resource.toURI()),
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

    @Test
    void testRouteWithReplacementSymbol() throws Exception {
        URL resource =
                Resources.getResource("definitions/pipeline-definition-full-with-repsym.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(new Path(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDefWithRouteRepSym);
    }

    @Test
    void testUdfDefinition() throws Exception {
        URL resource = Resources.getResource("definitions/pipeline-definition-with-udf.yaml");
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(new Path(resource.toURI()), new Configuration());
        assertThat(pipelineDef).isEqualTo(pipelineDefWithUdf);
    }

    @Test
    void testSchemaEvolutionTypesConfiguration() throws Exception {
        testSchemaEvolutionTypesParsing(
                "evolve",
                null,
                null,
                ImmutableSet.of(
                        ADD_COLUMN,
                        ALTER_COLUMN_TYPE,
                        CREATE_TABLE,
                        DROP_COLUMN,
                        DROP_TABLE,
                        RENAME_COLUMN,
                        TRUNCATE_TABLE));
        testSchemaEvolutionTypesParsing(
                "try_evolve",
                null,
                null,
                ImmutableSet.of(
                        ADD_COLUMN,
                        ALTER_COLUMN_TYPE,
                        CREATE_TABLE,
                        DROP_COLUMN,
                        DROP_TABLE,
                        RENAME_COLUMN,
                        TRUNCATE_TABLE));
        testSchemaEvolutionTypesParsing(
                "evolve",
                "[column, table]",
                "[drop]",
                ImmutableSet.of(
                        ADD_COLUMN,
                        ALTER_COLUMN_TYPE,
                        CREATE_TABLE,
                        RENAME_COLUMN,
                        TRUNCATE_TABLE));
        testSchemaEvolutionTypesParsing(
                "lenient",
                null,
                null,
                ImmutableSet.of(
                        ADD_COLUMN, ALTER_COLUMN_TYPE, CREATE_TABLE, DROP_COLUMN, RENAME_COLUMN));
        testSchemaEvolutionTypesParsing(
                "lenient",
                null,
                "[]",
                ImmutableSet.of(
                        ADD_COLUMN,
                        ALTER_COLUMN_TYPE,
                        CREATE_TABLE,
                        DROP_COLUMN,
                        DROP_TABLE,
                        RENAME_COLUMN,
                        TRUNCATE_TABLE));
    }

    private void testSchemaEvolutionTypesParsing(
            String behavior, String included, String excluded, Set<SchemaChangeEventType> expected)
            throws Exception {
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                parser.parse(
                        "source:\n"
                                + "  type: foo\n"
                                + "sink:\n"
                                + "  type: bar\n"
                                + (included != null
                                        ? String.format("  include.schema.changes: %s\n", included)
                                        : "")
                                + (excluded != null
                                        ? String.format("  exclude.schema.changes: %s\n", excluded)
                                        : "")
                                + "pipeline:\n"
                                + "  schema.change.behavior: "
                                + behavior
                                + "\n"
                                + "  parallelism: 1\n",
                        new Configuration());
        assertThat(pipelineDef)
                .isEqualTo(
                        new PipelineDef(
                                new SourceDef("foo", null, new Configuration()),
                                new SinkDef("bar", null, new Configuration(), expected),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Configuration.fromMap(
                                        ImmutableMap.<String, String>builder()
                                                .put("schema.change.behavior", behavior)
                                                .put("parallelism", "1")
                                                .build())));
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
                                    null,
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order",
                                    null,
                                    "sync table to with given prefix ods_")),
                    Arrays.asList(
                            new TransformDef(
                                    "mydb.app_order_.*",
                                    "id, order_id, TO_UPPER(product_name)",
                                    "id > 10 AND order_id > 100",
                                    "id",
                                    "product_name",
                                    "comment=app order",
                                    "project fields from source table",
                                    "SOFT_DELETE"),
                            new TransformDef(
                                    "mydb.web_order_.*",
                                    "CONCAT(id, order_id) as uniq_id, *",
                                    "uniq_id > 10",
                                    null,
                                    null,
                                    null,
                                    "add new uniq_id for each row",
                                    null)),
                    Collections.emptyList(),
                    Collections.singletonList(
                            new ModelDef(
                                    "GET_EMBEDDING",
                                    "OpenAIEmbeddingModel",
                                    new LinkedHashMap<>(
                                            ImmutableMap.<String, String>builder()
                                                    .put("model-name", "GET_EMBEDDING")
                                                    .put("class-name", "OpenAIEmbeddingModel")
                                                    .put("openai.model", "text-embedding-3-small")
                                                    .put("openai.host", "https://xxxx")
                                                    .put("openai.apikey", "abcd1234")
                                                    .build()))),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("execution.runtime-mode", "STREAMING")
                                    .put("schema.change.behavior", "evolve")
                                    .put("schema-operator.rpc-timeout", "1 h")
                                    .build()));

    @Test
    void testParsingFullDefinitionFromString() throws Exception {
        String pipelineDefText =
                "source:\n"
                        + "  type: mysql\n"
                        + "  name: source-database\n"
                        + "  host: localhost\n"
                        + "  port: 3306\n"
                        + "  username: admin\n"
                        + "  password: pass\n"
                        + "  tables: adb.*, bdb.user_table_[0-9]+, [app|web]_order_.*\n"
                        + "  chunk-column: app_order_.*:id,web_order:product_id\n"
                        + "  capture-new-tables: true\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: kafka\n"
                        + "  name: sink-queue\n"
                        + "  bootstrap-servers: localhost:9092\n"
                        + "  auto-create-table: true\n"
                        + "\n"
                        + "route:\n"
                        + "  - source-table: mydb.default.app_order_.*\n"
                        + "    sink-table: odsdb.default.app_order\n"
                        + "    description: sync all sharding tables to one\n"
                        + "  - source-table: mydb.default.web_order\n"
                        + "    sink-table: odsdb.default.ods_web_order\n"
                        + "    description: sync table to with given prefix ods_\n"
                        + "\n"
                        + "transform:\n"
                        + "  - source-table: mydb.app_order_.*\n"
                        + "    projection: id, order_id, TO_UPPER(product_name)\n"
                        + "    filter: id > 10 AND order_id > 100\n"
                        + "    primary-keys: id\n"
                        + "    partition-keys: product_name\n"
                        + "    table-options: comment=app order\n"
                        + "    description: project fields from source table\n"
                        + "    converter-after-transform: SOFT_DELETE\n"
                        + "  - source-table: mydb.web_order_.*\n"
                        + "    projection: CONCAT(id, order_id) as uniq_id, *\n"
                        + "    filter: uniq_id > 10\n"
                        + "    description: add new uniq_id for each row\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  name: source-database-sync-pipe\n"
                        + "  parallelism: 4\n"
                        + "  schema.change.behavior: evolve\n"
                        + "  schema-operator.rpc-timeout: 1 h\n"
                        + "  execution.runtime-mode: STREAMING\n"
                        + "  model:\n"
                        + "    - model-name: GET_EMBEDDING\n"
                        + "      class-name: OpenAIEmbeddingModel\n"
                        + "      openai.model: text-embedding-3-small\n"
                        + "      openai.host: https://xxxx\n"
                        + "      openai.apikey: abcd1234";
        YamlPipelineDefinitionParser parser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef = parser.parse(pipelineDefText, new Configuration());
        assertThat(pipelineDef).isEqualTo(fullDef);
    }

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
                                    null,
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order",
                                    null,
                                    "sync table to with given prefix ods_")),
                    Arrays.asList(
                            new TransformDef(
                                    "mydb.app_order_.*",
                                    "id, order_id, TO_UPPER(product_name)",
                                    "id > 10 AND order_id > 100",
                                    "id",
                                    "product_name",
                                    "comment=app order",
                                    "project fields from source table",
                                    "SOFT_DELETE"),
                            new TransformDef(
                                    "mydb.web_order_.*",
                                    "CONCAT(id, order_id) as uniq_id, *",
                                    "uniq_id > 10",
                                    null,
                                    null,
                                    null,
                                    "add new uniq_id for each row",
                                    null)),
                    Collections.emptyList(),
                    Collections.singletonList(
                            new ModelDef(
                                    "GET_EMBEDDING",
                                    "OpenAIEmbeddingModel",
                                    new LinkedHashMap<>(
                                            ImmutableMap.<String, String>builder()
                                                    .put("model-name", "GET_EMBEDDING")
                                                    .put("class-name", "OpenAIEmbeddingModel")
                                                    .put("openai.model", "text-embedding-3-small")
                                                    .put("openai.host", "https://xxxx")
                                                    .put("openai.apikey", "abcd1234")
                                                    .build()))),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("schema.change.behavior", "evolve")
                                    .put("schema-operator.rpc-timeout", "1 h")
                                    .put("execution.runtime-mode", "STREAMING")
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
                                            .build()),
                            ImmutableSet.of(
                                    DROP_COLUMN,
                                    ALTER_COLUMN_TYPE,
                                    ADD_COLUMN,
                                    CREATE_TABLE,
                                    RENAME_COLUMN)),
                    Collections.singletonList(
                            new RouteDef(
                                    "mydb.default.app_order_.*",
                                    "odsdb.default.app_order",
                                    null,
                                    null)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("parallelism", "4")
                                    .build()));

    private final PipelineDef minimizedDef =
            new PipelineDef(
                    new SourceDef("mysql", null, new Configuration()),
                    new SinkDef(
                            "kafka",
                            null,
                            new Configuration(),
                            ImmutableSet.of(
                                    DROP_COLUMN,
                                    ALTER_COLUMN_TYPE,
                                    ADD_COLUMN,
                                    CREATE_TABLE,
                                    RENAME_COLUMN)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Configuration.fromMap(
                            Collections.singletonMap(
                                    "local-time-zone", ZoneId.systemDefault().toString())));

    private final PipelineDef fullDefWithRouteRepSym =
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
                                    "odsdb.default.app_order_<>",
                                    "<>",
                                    "sync all sharding tables to one"),
                            new RouteDef(
                                    "mydb.default.web_order",
                                    "odsdb.default.ods_web_order_>_<",
                                    ">_<",
                                    "sync table to with given prefix ods_")),
                    Arrays.asList(
                            new TransformDef(
                                    "mydb.app_order_.*",
                                    "id, order_id, TO_UPPER(product_name)",
                                    "id > 10 AND order_id > 100",
                                    "id",
                                    "product_name",
                                    "comment=app order",
                                    "project fields from source table",
                                    "SOFT_DELETE"),
                            new TransformDef(
                                    "mydb.web_order_.*",
                                    "CONCAT(id, order_id) as uniq_id, *",
                                    "uniq_id > 10",
                                    null,
                                    null,
                                    null,
                                    "add new uniq_id for each row",
                                    null)),
                    Collections.emptyList(),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("name", "source-database-sync-pipe")
                                    .put("parallelism", "4")
                                    .put("schema.change.behavior", "evolve")
                                    .put("schema-operator.rpc-timeout", "1 h")
                                    .build()));

    private final PipelineDef pipelineDefWithUdf =
            new PipelineDef(
                    new SourceDef("values", null, new Configuration()),
                    new SinkDef(
                            "values",
                            null,
                            new Configuration(),
                            ImmutableSet.of(
                                    DROP_COLUMN,
                                    ALTER_COLUMN_TYPE,
                                    ADD_COLUMN,
                                    CREATE_TABLE,
                                    RENAME_COLUMN)),
                    Collections.emptyList(),
                    Collections.singletonList(
                            new TransformDef(
                                    "mydb.web_order",
                                    "*, inc(inc(inc(id))) as inc_id, format(id, 'id -> %d') as formatted_id",
                                    "inc(id) < 100",
                                    null,
                                    null,
                                    null,
                                    null,
                                    null)),
                    Arrays.asList(
                            new UdfDef(
                                    "inc",
                                    "org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass"),
                            new UdfDef(
                                    "format",
                                    "org.apache.flink.cdc.udf.examples.java.FormatFunctionClass")),
                    Configuration.fromMap(
                            ImmutableMap.<String, String>builder()
                                    .put("parallelism", "1")
                                    .build()));
}
