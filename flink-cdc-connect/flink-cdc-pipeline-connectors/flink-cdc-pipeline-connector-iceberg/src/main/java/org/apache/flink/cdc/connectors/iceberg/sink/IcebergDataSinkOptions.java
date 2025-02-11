package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

public class IcebergDataSinkOptions {

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
                    .noDefaultValue()
                    .withDescription("Metastore of iceberg catalog, supports filesystem and hive.");

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
                                    + "Tables are separated by ';', and partition keys are separated by ','. "
                                    + "For example, we can set partition.key of two tables by 'testdb.table1:id1,id2;testdb.table2:name'.");
}
