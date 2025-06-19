package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.sink.MetadataApplier;

import java.util.Set;

public class FlussMetaDataApplier implements MetadataApplier {
    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {}

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        return MetadataApplier.super.setAcceptedSchemaEvolutionTypes(schemaEvolutionTypes);
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return MetadataApplier.super.acceptsSchemaEvolutionType(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return MetadataApplier.super.getSupportedSchemaEvolutionTypes();
    }

    @Override
    public void close() throws Exception {
        MetadataApplier.super.close();
    }
}
