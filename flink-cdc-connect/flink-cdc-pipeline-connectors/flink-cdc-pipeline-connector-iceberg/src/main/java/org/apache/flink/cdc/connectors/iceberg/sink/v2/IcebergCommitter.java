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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.event.TableId;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/** A {@link Committer} for Apache Iceberg. */
public class IcebergCommitter implements Committer<WriteResultWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCommitter.class);

    private final Catalog catalog;

    public IcebergCommitter(Map<String, String> catalogOptions) {
        this.catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog", catalogOptions, new Configuration());
    }

    @Override
    public void commit(Collection<CommitRequest<WriteResultWrapper>> collection)
            throws IOException, InterruptedException {
        List<WriteResultWrapper> results =
                collection.stream().map(CommitRequest::getCommittable).collect(toList());
        commit(results);
    }

    private void commit(List<WriteResultWrapper> writeResultWrappers) {
        Map<TableId, List<WriteResult>> tableMap = new HashMap<>();
        for (WriteResultWrapper writeResultWrapper : writeResultWrappers) {
            List<WriteResult> writeResult =
                    tableMap.getOrDefault(writeResultWrapper.getTableId(), new ArrayList<>());
            writeResult.add(writeResultWrapper.getWriteResult());
            tableMap.put(writeResultWrapper.getTableId(), writeResult);
            LOGGER.info(writeResultWrapper.buildDescription());
        }
        for (Map.Entry<TableId, List<WriteResult>> entry : tableMap.entrySet()) {
            TableId tableId = entry.getKey();
            Table table =
                    catalog.loadTable(
                            TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));
            List<WriteResult> results = entry.getValue();
            List<DataFile> dataFiles =
                    results.stream()
                            .filter(payload -> payload.dataFiles() != null)
                            .flatMap(payload -> Arrays.stream(payload.dataFiles()))
                            .filter(dataFile -> dataFile.recordCount() > 0)
                            .collect(toList());

            List<DeleteFile> deleteFiles =
                    results.stream()
                            .filter(payload -> payload.deleteFiles() != null)
                            .flatMap(payload -> Arrays.stream(payload.deleteFiles()))
                            .filter(deleteFile -> deleteFile.recordCount() > 0)
                            .collect(toList());

            if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
                LOGGER.info(String.format("Nothing to commit to table %s, skipping", table.name()));
            } else {
                if (deleteFiles.isEmpty()) {
                    AppendFiles append = table.newAppend();
                    dataFiles.forEach(append::appendFile);
                    append.commit();
                } else {
                    RowDelta delta = table.newRowDelta();
                    dataFiles.forEach(delta::addRows);
                    deleteFiles.forEach(delta::addDeletes);
                    delta.commit();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {}
}
