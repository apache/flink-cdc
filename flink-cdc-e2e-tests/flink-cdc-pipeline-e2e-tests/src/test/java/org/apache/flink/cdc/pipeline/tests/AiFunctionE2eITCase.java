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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

/** E2e tests for AI functions with the dummy model and openai-compatible model. */
class AiFunctionE2eITCase extends PipelineTestEnvironment {

    @Test
    void testAiFunctionsWithDummyModel() throws Exception {
        String pipelineJob =
                "source:\n"
                        + "  type: values\n"
                        + "  event-set.id: SINGLE_SPLIT_MULTI_TABLES\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "\n"
                        + "transform:\n"
                        + "  - source-table: default_namespace.default_schema.table1\n"
                        + "    projection: col1, AI_COMPLETE('myModel', col1, 'Classify into catA or catB') AS cls\n"
                        + "  - source-table: default_namespace.default_schema.table2\n"
                        + "    projection: col1, AI_EMBED('myModel', col1) AS embedding\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: 1\n"
                        + "  schema.change.behavior: evolve\n"
                        + "  model:\n"
                        + "    name: myModel\n"
                        + "    type: dummy\n"
                        + "    debug: true\n";

        Path dummyModelJar = TestUtils.getResource("dummy-model.jar");
        submitPipelineJob(pipelineJob, dummyModelJar);
        waitUntilJobFinished(Duration.ofMinutes(3));

        validateResult(
                "Successfully opened AI model client 'myModel'.",
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`cls` VARIANT}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, {\"result\":\"dummy result\",\"summary\":\"TL;DR\"}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, {\"result\":\"dummy result\",\"summary\":\"TL;DR\"}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, {\"result\":\"dummy result\",\"summary\":\"TL;DR\"}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, {\"result\":\"dummy result\",\"summary\":\"TL;DR\"}], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, {\"result\":\"dummy result\",\"summary\":\"TL;DR\"}], after=[2, {\"result\":\"dummy result\",\"summary\":\"TL;DR\"}], op=UPDATE, meta=()}",
                "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING NOT NULL,`embedding` ARRAY<FLOAT>}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, [3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, [3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, [3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]], op=INSERT, meta=()}",
                "Successfully closed AI model client 'myModel'.");
    }

    @Test
    void testAiFunctionsWithOpenAiCompatibleModel() throws Exception {
        String endpoint = System.getenv("OPENAI_BASE_URL");
        String apiKey = System.getenv("OPENAI_API_KEY");
        String model = System.getenv("OPENAI_MODEL");
        Assumptions.assumeThat(endpoint != null && apiKey != null && model != null)
                .as("OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL must be set")
                .isTrue();

        String pipelineJob =
                "source:\n"
                        + "  type: values\n"
                        + "  event-set.id: SINGLE_SPLIT_MULTI_TABLES\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "\n"
                        + "transform:\n"
                        + "  - source-table: default_namespace.default_schema.table1\n"
                        + "    projection: col1, AI_COMPLETE('openaiModel', col1, 'Reply only the negative value of input number') AS reversed\n"
                        + "  - source-table: default_namespace.default_schema.table2\n"
                        + "    projection: col1, AI_SUMMARIZE('openaiModel', col1, 50) AS summary\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: 1\n"
                        + "  schema.change.behavior: evolve\n"
                        + "  model:\n"
                        + "    name: openaiModel\n"
                        + "    type: openai-compatible\n"
                        + "    endpoint: "
                        + endpoint
                        + "\n"
                        + "    api-key: "
                        + apiKey
                        + "\n"
                        + "    model-name: "
                        + model
                        + "\n";

        Path openaiModelJar = TestUtils.getResource("openai-compatible-model.jar");
        submitPipelineJob(pipelineJob, openaiModelJar);
        waitUntilJobFinished(Duration.ofMinutes(5));

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING NOT NULL,`reversed` VARIANT}, primaryKeys=col1, options=()}",
                "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING NOT NULL,`summary` VARIANT}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, {\"result\":-1}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, {\"result\":-2}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, {\"result\":-3}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, {\"summary\":\"1\"}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, {\"summary\":\"2\"}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, {\"summary\":\"3\"}], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, {\"result\":-1}], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, {\"result\":-2}], after=[2, {\"result\":-2}], op=UPDATE, meta=()}");
    }
}
