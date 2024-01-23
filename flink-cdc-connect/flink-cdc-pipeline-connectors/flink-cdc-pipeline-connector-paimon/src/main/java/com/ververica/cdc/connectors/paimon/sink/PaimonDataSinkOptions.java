/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.paimon.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import org.apache.paimon.options.CatalogOptions;

import static com.ververica.cdc.common.configuration.ConfigOptions.key;

/** copy from {@link CatalogOptions}. Options for {@link PaimonDataSink}. */
public class PaimonDataSinkOptions {

    // prefix for passing properties for table creation.
    public static final String PREFIX_TABLE_PROPERTIES = "table.properties.";

    // prefix for passing properties for catalog creation.
    public static final String PREFIX_CATALOG_PROPERTIES = "catalog.properties.";

    public static final ConfigOption<String> COMMIT_USER =
            key("commit.user")
                    .stringType()
                    .defaultValue("admin")
                    .withDescription("User name for committing data files.");

    public static final ConfigOption<String> WAREHOUSE =
            key("catalog.properties.warehouse")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The warehouse root path of catalog.");

    public static final ConfigOption<String> METASTORE =
            key("catalog.properties.metastore")
                    .stringType()
                    .defaultValue("filesystem")
                    .withDescription("Metastore of paimon catalog, supports filesystem and hive.");

    public static final ConfigOption<String> URI =
            key("catalog.properties.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Uri of metastore server.");

    public static final ConfigOption<String> PARTITION_KEY =
            key("partition.key")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Partition keys for each partitioned table, allow setting multiple primary keys for multiTables. "
                                    + "Each table are separated by ';', and each partition key are separated by ','. "
                                    + "For example, we can set partition.key of two tables by 'testdb.table1:id1,id2;testdb.table2:name'.");
}
