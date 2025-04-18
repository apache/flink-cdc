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

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.configuration.ConfigOption;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Config options for {@link IcebergDataSink}. */
public class IcebergDataSinkOptions {

    // prefix for passing properties for table creation.
    public static final String PREFIX_TABLE_PROPERTIES = "table.properties.";

    // prefix for passing properties for catalog creation.
    public static final String PREFIX_CATALOG_PROPERTIES = "catalog.properties.";

    public static final ConfigOption<String> TYPE =
            key("catalog.properties.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Type of iceberg catalog, supports `hadoop` and `hive`.");

    public static final ConfigOption<String> WAREHOUSE =
            key("catalog.properties.warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The warehouse root path of catalog, only usable when catalog.properties.type is `hadoop`.");

    public static final ConfigOption<String> PARTITION_KEY =
            key("partition.key")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Partition keys for each partitioned table, allow setting multiple primary keys for multiTables. "
                                    + "Tables are separated by ';', and partition keys are separated by ','. "
                                    + "For example, we can set partition.key of two tables by 'testdb.table1:id1,id2;testdb.table2:name'.");

    @Experimental
    public static final ConfigOption<Boolean> SINK_COMPACTION_ENABLED =
            key("sink.compaction.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable iceberg small file optimization. If there are too many tables after enabling it, data flow may be blocked."
                                    + " Please enable it carefully or use table maintenance service to compaction small file");

    @Experimental
    public static final ConfigOption<Integer> SINK_COMPACTION_COMMIT_INTERVAL =
            key("sink.compaction.commit.interval")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The commit interval for file compaction of each Iceberg table.");

    @Experimental
    public static final ConfigOption<Integer> SINK_COMPACTION_PARALLELISM =
            key("sink.compaction.commit.parallelism")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The parallelism for file compaction, default value is -1, which means that compaction parallelism is equal to sink writer parallelism.");
}
