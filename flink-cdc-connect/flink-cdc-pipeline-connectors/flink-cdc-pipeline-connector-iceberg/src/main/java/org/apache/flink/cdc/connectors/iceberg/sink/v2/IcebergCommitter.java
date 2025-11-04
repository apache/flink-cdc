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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

/** A {@link Committer} for Apache Iceberg. */
public class IcebergCommitter implements Committer<WriteResultWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCommitter.class);

    public static final String NAMESPACE_GROUP_KEY = "namespace";

    public static final String SCHEMA_GROUP_KEY = "schema";

    public static final String TABLE_GROUP_KEY = "table";

    private final Catalog catalog;

    private final SinkCommitterMetricGroup metricGroup;

    private final Map<TableId, TableMetric> tableIdMetricMap;

    public IcebergCommitter(Map<String, String> catalogOptions) {
        this(catalogOptions, null);
    }

    public IcebergCommitter(
            Map<String, String> catalogOptions, SinkCommitterMetricGroup metricGroup) {
        this.catalog =
                CatalogUtil.buildIcebergCatalog(
                        this.getClass().getSimpleName(), catalogOptions, new Configuration());
        this.metricGroup = metricGroup;
        this.tableIdMetricMap = new HashMap<>();
    }

    @Override
    public void commit(Collection<CommitRequest<WriteResultWrapper>> collection) {
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
            Optional<TableMetric> tableMetric = getTableMetric(tableId);
            tableMetric.ifPresent(TableMetric::increaseCommitTimes);
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

    private Optional<TableMetric> getTableMetric(TableId tableId) {
        if (tableIdMetricMap.containsKey(tableId)) {
            return Optional.of(tableIdMetricMap.get(tableId));
        } else {
            if (metricGroup == null) {
                return Optional.empty();
            }
            MetricGroup tableIdMetricGroup =
                    metricGroup
                            .addGroup(NAMESPACE_GROUP_KEY, tableId.getNamespace())
                            .addGroup(SCHEMA_GROUP_KEY, tableId.getSchemaName())
                            .addGroup(TABLE_GROUP_KEY, tableId.getTableName());
            TableMetric tableMetric = new TableMetric(tableIdMetricGroup);
            tableIdMetricMap.put(tableId, tableMetric);
            return Optional.of(tableMetric);
        }
    }

    @Override
    public void close() {}
}
