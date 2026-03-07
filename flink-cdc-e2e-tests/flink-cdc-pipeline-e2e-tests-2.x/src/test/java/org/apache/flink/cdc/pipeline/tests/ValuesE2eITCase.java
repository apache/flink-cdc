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

import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/** End-to-end tests for values cdc pipeline job. */
class ValuesE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(ValuesE2eITCase.class);

    @Test
    void testValuesSingleSplitSingleTable() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: SINGLE_SPLIT_SINGLE_TABLE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

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
    }

    @Test
    void testValuesSingleSplitMultiTables() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: SINGLE_SPLIT_MULTI_TABLES\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, 3], op=INSERT, meta=()}",
                "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, 2], after=[2, x], op=UPDATE, meta=()}");
    }

    @Test
    void testValuesMultiSplitsSingleTable() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: MULTI_SPLITS_SINGLE_TABLE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[4, 4], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[5, 5], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[6, 6], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, 2], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[4, 4], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[6, 6], after=[], op=DELETE, meta=()}",
                "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1, ], after=[1, 1, x], op=UPDATE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[3, 3, ], after=[3, 3, x], op=UPDATE, meta=()}");
    }

    @Test
    void testValuesSingleSplitSingleTableWithDefaultValue() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: SINGLE_SPLIT_SINGLE_TABLE_WITH_DEFAULT_VALUE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`col3` STRING, position=LAST, existedColumnName=null}]}",
                "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={col2=newCol2, col3=newCol3}}",
                "DropColumnEvent{tableId=default_namespace.default_schema.table1, droppedColumnNames=[newCol2]}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[1, 1], after=[], op=DELETE, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[2, ], after=[2, x], op=UPDATE, meta=()}",
                "AddColumnEvent{tableId=default_namespace.default_schema.table1, addedColumns=[ColumnWithPosition{column=`colWithDefault` STRING 'flink', position=LAST, existedColumnName=null}]}",
                "RenameColumnEvent{tableId=default_namespace.default_schema.table1, nameMapping={colWithDefault=newColWithDefault}}");
    }

    @Test
    void testValuesSingleSplitSingleBatchTable() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: SINGLE_SPLIT_SINGLE_BATCH_TABLE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}");
    }

    @Test
    void testValuesSingleSplitMultiBatchTable() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: SINGLE_SPLIT_MULTI_BATCH_TABLE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "CreateTableEvent{tableId=default_namespace.default_schema.table2, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table2, before=[], after=[3, 3], op=INSERT, meta=()}");
    }

    @Test
    void testValuesMultiSplitsSingleBatchTable() throws Exception {
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: values\n"
                                + "  event-set.id: MULTI_SPLITS_SINGLE_BATCH_TABLE\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: values\n"
                                + "  print.enabled: true\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d\n"
                                + "  execution.runtime-mode: BATCH",
                        parallelism);

        submitPipelineJob(pipelineJob);
        waitUntilJobFinished(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateResult(
                "CreateTableEvent{tableId=default_namespace.default_schema.table1, schema=columns={`col1` STRING,`col2` STRING}, primaryKeys=col1, options=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[1, 1], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[2, 2], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[3, 3], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[4, 4], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[5, 5], op=INSERT, meta=()}",
                "DataChangeEvent{tableId=default_namespace.default_schema.table1, before=[], after=[6, 6], op=INSERT, meta=()}");
    }
}
