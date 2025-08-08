package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.hudi.sink.v2.HudiSink;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;

/**
 * A {@link DataSink} for Apache Hudi that provides the main entry point for the Flink CDC
 * framework.
 */
public class HudiDataSink implements DataSink, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HudiDataSink.class);

    private final Configuration config;

    private final String schemaOperatorUid;

    public HudiDataSink(Configuration config, String schemaOperatorUid) {
        LOG.info("Creating HudiDataSink with universal configuration {}", config);
        this.config = config;
        this.schemaOperatorUid = schemaOperatorUid;
    }

    /** Provides the core sink implementation that handles the data flow of events. */
    @Override
    public EventSinkProvider getEventSinkProvider() {
        LOG.info("Creating HudiDataSinkProvider with universal configuration {}", config);
        // For CDC pipelines, we don't have a pre-configured schema since tables are created
        // dynamically
        // Instead, we use a multi-table sink that handles schema discovery and table creation

        // Convert CDC configuration to Flink configuration for HoodieSink
        org.apache.flink.configuration.Configuration flinkConfig = toFlinkConfig(config);

        // Extract configuration options
        java.util.Map<String, String> configMap = config.toMap();
        boolean overwrite = "insert_overwrite".equals(configMap.get("write.operation"));
        boolean isBounded = "BATCH".equals(configMap.get("execution.checkpointing.mode"));

        // Create the HudiSink with multi-table support via wrapper pattern
        // Use empty RowType since tables are created dynamically
        HudiSink hudiSink =
                new HudiSink(
                        flinkConfig,
                        RowType.of(), // Empty row type for dynamic multi-table support
                        overwrite,
                        isBounded,
                        schemaOperatorUid,
                        ZoneId.systemDefault());

        return FlinkSinkProvider.of(hudiSink);
    }

    /**
     * Provides the metadata applier. In our design, this has a passive role (e.g., logging), as
     * transactional metadata operations are handled by the HudiCommitter.
     */
    @Override
    public MetadataApplier getMetadataApplier() {
        return new HudiMetadataApplier(config);
    }

    /**
     * Converts a {@link org.apache.flink.cdc.common.configuration.Configuration} to a {@link
     * org.apache.flink.configuration.Configuration}.
     *
     * @param cdcConfig The input CDC configuration.
     * @return A new Flink configuration containing the same key-value pairs.
     */
    private static org.apache.flink.configuration.Configuration toFlinkConfig(
            Configuration cdcConfig) {
        final org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        if (cdcConfig != null) {
            cdcConfig.toMap().forEach(flinkConfig::setString);
        }
        return flinkConfig;
    }
}
