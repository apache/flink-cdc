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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RefreshingTableLoaderTest {
    @TempDir Path directory;

    @Test
    void protectsEmptyTableAndRefreshesSnapshotsAfterExternalCommitsAndExpiration()
            throws Exception {
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), directory.toString())) {
            Table writerTable =
                    catalog.createTable(
                            TableIdentifier.of("sales", "orders"),
                            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
            try (TableLoader loader =
                    new RefreshingDeleteOrphanFilesBuilder.RefreshingTableLoader(
                            TableLoader.fromHadoopTable(
                                    writerTable.location(),
                                    new org.apache.hadoop.conf.Configuration()))) {
                loader.open();
                Table maintenanceTable = loader.loadTable();
                assertThatThrownBy(maintenanceTable::snapshots)
                        .hasMessageContaining("deferred until")
                        .hasMessageContaining("committed snapshot");
                writerTable.newAppend().commit();
                long first = writerTable.currentSnapshot().snapshotId();
                assertThat(maintenanceTable.snapshots())
                        .extracting(Snapshot::snapshotId)
                        .containsExactly(first);
                writerTable.updateProperties().set(TableProperties.GC_ENABLED, "false").commit();
                assertThatThrownBy(maintenanceTable::snapshots).hasMessageContaining("gc.enabled");
                writerTable.updateProperties().set(TableProperties.GC_ENABLED, "true").commit();
                writerTable.newAppend().commit();
                long second = writerTable.currentSnapshot().snapshotId();
                writerTable.expireSnapshots().expireSnapshotId(first).commit();
                assertThat(maintenanceTable.snapshots())
                        .extracting(Snapshot::snapshotId)
                        .containsExactly(second);
                try (TableLoader cloned = loader.clone()) {
                    cloned.open();
                    assertThat(cloned.loadTable().snapshots())
                            .extracting(Snapshot::snapshotId)
                            .containsExactly(second);
                }
            }
        }
    }
}
