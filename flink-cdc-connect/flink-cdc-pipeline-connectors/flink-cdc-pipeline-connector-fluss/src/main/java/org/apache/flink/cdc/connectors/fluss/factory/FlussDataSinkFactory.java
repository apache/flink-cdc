package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.fluss.config.FlussSinkOptions;
import org.apache.flink.cdc.connectors.fluss.sink.FlussDataSink;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.DATABASE;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.PREFIX_FLUSS_PROPERTIES;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.SHUFFLE_BY_BUCKET_ID;
import static org.apache.flink.cdc.connectors.fluss.sink.FlussDataSinkOptions.TABLE;

public class FlussDataSinkFactory implements DataSinkFactory {
    public static final String IDENTIFIER = "fluss";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validateExcept(PREFIX_FLUSS_PROPERTIES);

        Configuration configuration = context.getFactoryConfiguration();
        String bootstrap = configuration.get(BOOTSTRAP_SERVERS);
        String database = configuration.get(DATABASE);
        String table = configuration.get(TABLE);

        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        Map<String, String> flussOptions = new HashMap<>();
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(PREFIX_FLUSS_PROPERTIES)) {
                        flussOptions.put(key.substring(PREFIX_FLUSS_PROPERTIES.length()), value);
                    }
                });
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        FlussSinkOptions flussSinkOptions =
                new FlussSinkOptions(bootstrap, database, table, flussOptions);

        return new FlussDataSink(flussSinkOptions, zoneId);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        options.add(DATABASE);
        options.add(TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SHUFFLE_BY_BUCKET_ID);
        return options;
    }
}
