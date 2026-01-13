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

package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Tests for {@link IcebergDataSinkFactory}. */
public class IcebergDataSinkFactoryTest {
    @TempDir public static java.nio.file.Path temporaryFolder;

    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("iceberg", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(IcebergDataSinkFactory.class);

        Configuration conf = Configuration.fromMap(ImmutableMap.<String, String>builder().build());
        conf.set(IcebergDataSinkOptions.WAREHOUSE, "/tmp/warehouse");
        conf.set(IcebergDataSinkOptions.SINK_COMPACTION_PARALLELISM, 4);
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(IcebergDataSink.class);
    }

    @Test
    void testUnsupportedOption() {

        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("iceberg", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(IcebergDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("unsupported_key", "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'iceberg'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testPrefixRequireOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("iceberg", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(IcebergDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("catalog.properties.type", "hadoop")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(IcebergDataSink.class);
    }

    @Test
    public void testPartitionOption() {
        Map<String, String> testcases = new HashMap<>();
        testcases.put("test.iceberg_partition_table:year(create_time)", "create_time_year");
        testcases.put("test.iceberg_partition_table:month(create_time)", "create_time_month");
        testcases.put("test.iceberg_partition_table:day(create_time)", "create_time_day");
        testcases.put("test.iceberg_partition_table:hour(create_time)", "create_time_hour");
        testcases.put("test.iceberg_partition_table:bucket[8](create_time)", "create_time_bucket");
        testcases.put("test.iceberg_partition_table:create_time", "create_time");
        testcases.put("test.iceberg_partition_table:truncate[8](id)", "id_trunc");

        for (Map.Entry<String, String> entry : testcases.entrySet()) {
            runTestPartitionOption(entry.getKey(), entry.getValue());
        }
    }

    public void runTestPartitionOption(String partitionKey, String transformColumnName) {
        Map<String, String> catalogOptions = new HashMap<>();
        String warehouse =
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");
        Catalog catalog =
                CatalogUtil.buildIcebergCatalog(
                        "cdc-iceberg-catalog",
                        catalogOptions,
                        new org.apache.hadoop.conf.Configuration());

        Map<String, String> catalogConf = new HashMap<>();
        for (Map.Entry<String, String> entry : catalogOptions.entrySet()) {
            catalogConf.put(
                    IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES + entry.getKey(),
                    entry.getValue());
        }
        IcebergDataSinkFactory sinkFactory = new IcebergDataSinkFactory();
        Configuration conf = Configuration.fromMap(catalogConf);
        conf.set(IcebergDataSinkOptions.PARTITION_KEY, partitionKey);
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));

        TableId tableId = TableId.parse("test.iceberg_partition_table");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        DataTypes.BIGINT().notNull(),
                                        "column for id",
                                        "AUTO_DECREMENT()")
                                .physicalColumn(
                                        "create_time",
                                        DataTypes.TIMESTAMP().notNull(),
                                        "column for name",
                                        null)
                                .primaryKey("id")
                                .build());

        dataSink.getMetadataApplier().applySchemaChange(createTableEvent);

        Table table =
                catalog.loadTable(
                        TableIdentifier.of(tableId.getSchemaName(), tableId.getTableName()));

        Assertions.assertThat(table.specs().size()).isEqualTo(1);
        PartitionSpec spec = table.specs().get(0);
        Assertions.assertThat(spec.isPartitioned()).isTrue();
        Assertions.assertThat(spec.rawPartitionType().field(transformColumnName)).isNotNull();
    }
}
