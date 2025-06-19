package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

public class FlussDataSinkOptions {

    // prefix for passing properties for fluss options.
    public static final String PREFIX_FLUSS_PROPERTIES = "fluss.properties.";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The bootstrap servers for the Fluss sink connection");

    public static final ConfigOption<String> DATABASE =
            key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The database name for the Fluss sink");

    public static final ConfigOption<String> TABLE =
            key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table name for the Fluss sink");

    public static final ConfigOption<Boolean> SHUFFLE_BY_BUCKET_ID =
            key("shuffle-by-bucket-id")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to shuffle data by bucket ID");
}
