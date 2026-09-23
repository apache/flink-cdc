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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkFactory;
import org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkOptions;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.configuration.CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL;

/** The ITCase for writing to append-only tables using custom test source. */
public class AppendOnlyTableITCase {

    @TempDir public static java.nio.file.Path temporaryFolder;

    String warehouse;

    private static final int PARALLELISM = 4;

    private static final TableId TEST_TABLE_ID = TableId.tableId("default_database", "table1");

    // Always use parent-first classloader for CDC classes.
    // The reason is that test source uses static field for holding data, we need to make sure
    // the class is loaded by AppClassloader so that we can verify data in the test case.
    private static final org.apache.flink.configuration.Configuration MINI_CLUSTER_CONFIG =
            new org.apache.flink.configuration.Configuration();

    static {
        MINI_CLUSTER_CONFIG.set(
                ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                Collections.singletonList("org.apache.flink.cdc"));
    }

    /**
     * Use {@link MiniClusterExtension} to reduce the overhead of restarting the MiniCluster for
     * every test case.
     */
    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setConfiguration(MINI_CLUSTER_CONFIG)
                            .build());

    @BeforeEach
    void init() {
        warehouse = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
    }

    @Test
    void testWritingToAppendOnlyTable() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        runPipelineJob(false);

        Table table = catalog.getTable(Identifier.fromString(TEST_TABLE_ID.identifier()));
        List<String> results = getResultsFromTable(table, false);
        Assertions.assertThat(results)
                .containsExactlyInAnyOrder(
                        "default_database.table1:uuid=550e8400-e29b-41d4-a716-446655440000;path=oss://my-bucket/data/files/document_001.pdf;blobContent=PDF binary content for document 001",
                        "default_database.table1:uuid=6ba7b810-9dad-11d1-80b4-00c04fd430c8;path=oss://my-bucket/data/images/photo_002.jpg;blobContent=JPG binary content for photo 002",
                        "default_database.table1:uuid=f47ac10b-58cc-4372-a567-0e02b2c3d479;path=oss://my-bucket/data/videos/movie_003.mp4;blobContent=MP4 binary content for movie 003");
    }

    @Test
    void testWritingToAppendOnlyTableWithBlobDescriptor() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.setString("warehouse", warehouse);
        catalogOptions.setString("cache-enabled", "false");
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        runPipelineJob(true);

        Table table = catalog.getTable(Identifier.fromString(TEST_TABLE_ID.identifier()));
        List<String> results = getResultsFromTable(table, true);
        Assertions.assertThat(results)
                .containsExactlyInAnyOrder(
                        "default_database.table1:uuid=550e8400-e29b-41d4-a716-446655440000;path=oss://my-bucket/data/files/document_001.pdf;blobContent=PDF binary content for document 001",
                        "default_database.table1:uuid=6ba7b810-9dad-11d1-80b4-00c04fd430c8;path=oss://my-bucket/data/images/photo_002.jpg;blobContent=JPG binary content for photo 002",
                        "default_database.table1:uuid=f47ac10b-58cc-4372-a567-0e02b2c3d479;path=oss://my-bucket/data/videos/movie_003.mp4;blobContent=MP4 binary content for movie 003");
    }

    private List<String> getResultsFromTable(Table table, boolean withBlobDescriptor)
            throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        TableRead read = readBuilder.newRead();
        List<String> result = new ArrayList<>();
        RecordReader<InternalRow> reader = read.createReader(splits);
        reader.forEachRemaining(
                record -> {
                    String uuid = record.getString(0).toString();

                    String path =
                            withBlobDescriptor
                                    ? record.getBlob(1).toDescriptor().uri()
                                    : record.getString(1).toString();
                    String blobContent = new String(record.getBinary(2));
                    result.add(
                            String.format(
                                    "%s:uuid=%s;path=%s;blobContent=%s",
                                    TEST_TABLE_ID.identifier(), uuid, path, blobContent));
                });
        return result;
    }

    private void runPipelineJob(boolean withBlobDescriptor) throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

        // Setup values source with APPEND_ONLY_BLOB_TABLE events
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                ValuesDataSourceOptions.EVENT_SET_ID,
                ValuesDataSourceHelper.EventSetId.APPEND_ONLY_BLOB_TABLE);
        SourceDef sourceDef =
                new SourceDef(
                        ValuesDataFactory.IDENTIFIER, "Append Only Table Source", sourceConfig);

        // Setup pipeline
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, PARALLELISM);
        pipelineConfig.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, "Asia/Shanghai");

        // Setup paimon sink
        Map<String, String> sinkOptions = new HashMap<>();
        sinkOptions.put("table.properties.parquet.use-native", "false");
        if (withBlobDescriptor) {
            sinkOptions.put("table.properties.row-tracking.enabled", "true");
            sinkOptions.put("table.properties.data-evolution.enabled", "true");
            sinkOptions.put("table.properties.blob-field", "path");
            sinkOptions.put("table.properties.blob-descriptor-field", "path");
            sinkOptions.put("table.properties.blob-as-descriptor", "true");
        }
        Configuration sinkConfig = Configuration.fromMap(sinkOptions);
        sinkConfig.set(PaimonDataSinkOptions.METASTORE, "filesystem");
        sinkConfig.set(PaimonDataSinkOptions.WAREHOUSE, warehouse);
        SinkDef sinkDef = new SinkDef(PaimonDataSinkFactory.IDENTIFIER, "Paimon Sink", sinkConfig);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        // Execute the pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        execution.execute();
    }
}
