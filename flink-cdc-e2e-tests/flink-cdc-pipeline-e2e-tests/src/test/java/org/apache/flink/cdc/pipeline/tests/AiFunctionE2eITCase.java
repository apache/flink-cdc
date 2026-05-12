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

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

/** E2e tests for AI functions with the dummy model SPI. */
class AiFunctionE2eITCase extends PipelineTestEnvironment {

    private static final String TABLE_1 = "default_namespace.default_schema.table1";
    private static final String TABLE_2 = "default_namespace.default_schema.table2";
    private static final String DUMMY_JSON =
            "{\"result\":\"dummy result\",\"summary\":\"TL;DR\"}";
    private static final String DUMMY_EMBEDDING = "[3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0]";

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
                        + "  - source-table: "
                        + TABLE_1
                        + "\n"
                        + "    projection: col1, AI_COMPLETE('myModel', col1, 'Classify into catA or catB') AS cls\n"
                        + "  - source-table: "
                        + TABLE_2
                        + "\n"
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

        validateResult("Successfully opened AI model client 'myModel'.");
        validateResult(
                "CreateTableEvent{tableId="
                        + TABLE_1
                        + ", schema=columns={`col1` STRING NOT NULL,`cls` VARIANT}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId="
                        + TABLE_1
                        + ", before=[], after=[1, "
                        + DUMMY_JSON
                        + "], op=INSERT, meta=()}",
                "DataChangeEvent{tableId="
                        + TABLE_1
                        + ", before=[], after=[2, "
                        + DUMMY_JSON
                        + "], op=INSERT, meta=()}",
                "DataChangeEvent{tableId="
                        + TABLE_1
                        + ", before=[], after=[3, "
                        + DUMMY_JSON
                        + "], op=INSERT, meta=()}",
                "DataChangeEvent{tableId="
                        + TABLE_1
                        + ", before=[1, "
                        + DUMMY_JSON
                        + "], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId="
                        + TABLE_1
                        + ", before=[2, "
                        + DUMMY_JSON
                        + "], after=[2, "
                        + DUMMY_JSON
                        + "], op=UPDATE, meta=()}");

        validateResult(
                "CreateTableEvent{tableId="
                        + TABLE_2
                        + ", schema=columns={`col1` STRING NOT NULL,`embedding` ARRAY<FLOAT>}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId="
                        + TABLE_2
                        + ", before=[], after=[1, "
                        + DUMMY_EMBEDDING
                        + "], op=INSERT, meta=()}",
                "DataChangeEvent{tableId="
                        + TABLE_2
                        + ", before=[], after=[2, "
                        + DUMMY_EMBEDDING
                        + "], op=INSERT, meta=()}",
                "DataChangeEvent{tableId="
                        + TABLE_2
                        + ", before=[], after=[3, "
                        + DUMMY_EMBEDDING
                        + "], op=INSERT, meta=()}");
        validateResult("Successfully closed AI model client 'myModel'.");
    }
}
