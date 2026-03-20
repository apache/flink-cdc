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

package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatNoException;

/** Tests for {@link HudiMetadataApplier}. */
class HudiMetadataApplierTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    void testApplySchemaChange() throws Exception {
        String basePath =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Files.createDirectory(Paths.get(basePath));

        Configuration config = new Configuration();
        config.set(HudiConfig.PATH, basePath);
        config.set(HudiConfig.TABLE_TYPE, "MERGE_ON_READ");

        try (MetadataApplier metadataApplier = new HudiMetadataApplier(config)) {
            // Test create table
            CreateTableEvent createTableEvent =
                    new CreateTableEvent(
                            TableId.parse("test.table1"),
                            org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                    .physicalColumn(
                                            "col1",
                                            org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                    .notNull())
                                    .physicalColumn(
                                            "col2",
                                            org.apache.flink.cdc.common.types.DataTypes.INT())
                                    .primaryKey("col1")
                                    .build());
            metadataApplier.applySchemaChange(createTableEvent);

            // Test add column
            List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
            addedColumns.add(
                    new AddColumnEvent.ColumnWithPosition(
                            Column.physicalColumn(
                                    "col3",
                                    org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                    null,
                                    "col3DefValue")));
            AddColumnEvent addColumnEvent =
                    new AddColumnEvent(TableId.parse("test.table1"), addedColumns);
            assertThatNoException()
                    .isThrownBy(() -> metadataApplier.applySchemaChange(addColumnEvent));

            // Test rename column
            Map<String, String> nameMapping = new HashMap<>();
            nameMapping.put("col2", "newcol2");
            nameMapping.put("col3", "newcol3");
            RenameColumnEvent renameColumnEvent =
                    new RenameColumnEvent(TableId.parse("test.table1"), nameMapping);
            assertThatNoException()
                    .isThrownBy(() -> metadataApplier.applySchemaChange(renameColumnEvent));

            // Test alter column type
            Map<String, DataType> typeMapping = new HashMap<>();
            typeMapping.put("newcol2", org.apache.flink.cdc.common.types.DataTypes.STRING());
            AlterColumnTypeEvent alterColumnTypeEvent =
                    new AlterColumnTypeEvent(TableId.parse("test.table1"), typeMapping);
            assertThatNoException()
                    .isThrownBy(() -> metadataApplier.applySchemaChange(alterColumnTypeEvent));

            // Test drop column
            DropColumnEvent dropColumnEvent =
                    new DropColumnEvent(
                            TableId.parse("test.table1"), Collections.singletonList("newcol2"));
            assertThatNoException()
                    .isThrownBy(() -> metadataApplier.applySchemaChange(dropColumnEvent));
        }
    }

    @Test
    void testCreateTableWithAllDataTypes() throws Exception {
        String basePath =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Files.createDirectory(Paths.get(basePath));

        Configuration config = new Configuration();
        config.set(HudiConfig.PATH, basePath);
        config.set(HudiConfig.TABLE_TYPE, "MERGE_ON_READ");

        try (HudiMetadataApplier metadataApplier = new HudiMetadataApplier(config)) {
            CreateTableEvent createTableEvent =
                    new CreateTableEvent(
                            TableId.parse("test.table1"),
                            org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                    .physicalColumn(
                                            "col1",
                                            org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                    .notNull())
                                    .physicalColumn(
                                            "boolean",
                                            org.apache.flink.cdc.common.types.DataTypes.BOOLEAN())
                                    .physicalColumn(
                                            "binary",
                                            org.apache.flink.cdc.common.types.DataTypes.BINARY(3))
                                    .physicalColumn(
                                            "varbinary",
                                            org.apache.flink.cdc.common.types.DataTypes.VARBINARY(
                                                    10))
                                    .physicalColumn(
                                            "bytes",
                                            org.apache.flink.cdc.common.types.DataTypes.BYTES())
                                    .physicalColumn(
                                            "tinyint",
                                            org.apache.flink.cdc.common.types.DataTypes.TINYINT())
                                    .physicalColumn(
                                            "smallint",
                                            org.apache.flink.cdc.common.types.DataTypes.SMALLINT())
                                    .physicalColumn(
                                            "int",
                                            org.apache.flink.cdc.common.types.DataTypes.INT())
                                    .physicalColumn(
                                            "float",
                                            org.apache.flink.cdc.common.types.DataTypes.FLOAT())
                                    .physicalColumn(
                                            "double",
                                            org.apache.flink.cdc.common.types.DataTypes.DOUBLE())
                                    .physicalColumn(
                                            "decimal",
                                            org.apache.flink.cdc.common.types.DataTypes.DECIMAL(
                                                    6, 3))
                                    .physicalColumn(
                                            "char",
                                            org.apache.flink.cdc.common.types.DataTypes.CHAR(5))
                                    .physicalColumn(
                                            "varchar",
                                            org.apache.flink.cdc.common.types.DataTypes.VARCHAR(10))
                                    .physicalColumn(
                                            "string",
                                            org.apache.flink.cdc.common.types.DataTypes.STRING())
                                    .physicalColumn(
                                            "date",
                                            org.apache.flink.cdc.common.types.DataTypes.DATE())
                                    .physicalColumn(
                                            "time",
                                            org.apache.flink.cdc.common.types.DataTypes.TIME())
                                    .physicalColumn(
                                            "timestamp",
                                            org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP())
                                    .primaryKey("col1")
                                    .build());
            assertThatNoException()
                    .isThrownBy(() -> metadataApplier.applySchemaChange(createTableEvent));
        }
    }

    @Test
    void testClose() throws Exception {
        String basePath =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Files.createDirectory(Paths.get(basePath));

        Configuration config = new Configuration();
        config.set(HudiConfig.PATH, basePath);
        config.set(HudiConfig.TABLE_TYPE, "MERGE_ON_READ");

        HudiMetadataApplier metadataApplier = new HudiMetadataApplier(config);
        assertThatNoException().isThrownBy(metadataApplier::close);
    }
}
