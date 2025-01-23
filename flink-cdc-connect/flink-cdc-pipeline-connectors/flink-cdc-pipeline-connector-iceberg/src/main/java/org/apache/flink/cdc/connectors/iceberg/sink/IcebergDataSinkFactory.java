package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergRecordEventSerializer;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergRecordSerializer;
import org.apache.flink.table.catalog.Catalog;

import org.apache.commons.collections.map.HashedMap;
import org.apache.iceberg.flink.FlinkCatalogFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES;
import static org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.PREFIX_TABLE_PROPERTIES;

public class IcebergDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "iceberg";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PREFIX_TABLE_PROPERTIES, PREFIX_CATALOG_PROPERTIES);

        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        Map<String, String> catalogOptions = new HashMap<>();
        Map<String, String> tableOptions = new HashMap<>();
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(PREFIX_TABLE_PROPERTIES)) {
                        tableOptions.put(key.substring(PREFIX_TABLE_PROPERTIES.length()), value);
                    } else if (key.startsWith(IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES)) {
                        catalogOptions.put(
                                key.substring(
                                        IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES.length()),
                                value);
                    }
                });
        FlinkCatalogFactory factory = new FlinkCatalogFactory();
        try {
            Catalog catalog =
                    factory.createCatalog(
                            catalogOptions.getOrDefault("default-database", "default"),
                            catalogOptions);
            Preconditions.checkNotNull(
                    catalog.listDatabases(), "catalog option of Paimon is invalid.");
        } catch (Exception e) {
            throw new RuntimeException("failed to create or use paimon catalog", e);
        }
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        String commitUser =
                context.getFactoryConfiguration().get(IcebergDataSinkOptions.COMMIT_USER);
        IcebergRecordSerializer<Event> serializer =
                new IcebergRecordEventSerializer(new HashedMap(), zoneId);
        String schemaOperatorUid =
                context.getPipelineConfiguration()
                        .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);
        return new IcebergDataSink(
                catalogOptions,
                tableOptions,
                commitUser,
                new HashMap<>(),
                serializer,
                zoneId,
                schemaOperatorUid);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IcebergDataSinkOptions.METASTORE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IcebergDataSinkOptions.WAREHOUSE);
        options.add(IcebergDataSinkOptions.URI);
        options.add(IcebergDataSinkOptions.COMMIT_USER);
        options.add(IcebergDataSinkOptions.PARTITION_KEY);
        return options;
    }
}
