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

package org.apache.flink.cdc.pipeline.tests.migration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** E2e cases for stopping & restarting jobs. */
public class CrossVersionMigrationITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(CrossVersionMigrationITCase.class);

    @Test
    public void testGenericMigrationProcess() throws Exception {
        String content =
                "source:\n"
                        + "  type: values\n"
                        + "\n"
                        + "sink:\n"
                        + "  type: values\n"
                        + "\n"
                        + "pipeline:\n"
                        + "  parallelism: 1";
        JobID jobID = submitPipelineJob(content);
        Assertions.assertThat(jobID).isNotNull();
        LOG.info("Submitted Job with ID: {} ", jobID);

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, ], after=[2, x], op=UPDATE, meta=()}");

        String savepointPath = cancelJob(jobID);
        LOG.info("Stopped Job {} and saved savepoint to {}", jobID, savepointPath);
    }
}
