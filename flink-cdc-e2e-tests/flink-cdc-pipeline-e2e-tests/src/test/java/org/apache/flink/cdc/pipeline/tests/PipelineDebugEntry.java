package org.apache.flink.cdc.pipeline.tests; /*
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

import org.apache.flink.cdc.cli.CliFrontend;

/** A debug entry for running the pipeline in IDE. */
public class PipelineDebugEntry {
    public static void main(String[] args) throws Exception {
        String jobPath =
                "/Users/wuzexian/IdeaProjects/flink-cdc-2/flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests/src/test/java/org/apache/flink/cdc/pipeline/tests/test.yaml";
        String[] extArgs = {
            jobPath,
            "--use-mini-cluster",
            "true",
            "--jar",
            "/Users/wuzexian/IdeaProjects/flink-cdc-2/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-elasticsearch/target/flink-cdc-pipeline-connector-elasticsearch-3.2-SNAPSHOT.jar"
        };

        CliFrontend.main(extArgs);
    }
}
