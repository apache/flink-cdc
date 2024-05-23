package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

/** Unit test for {@link SchemaRegistryRequestHandler}. */
class SchemaRegistryRequestHandlerTest {

    private static final TableId CUSTOMERS =
            TableId.tableId("my_company", "my_branch", "customers");
    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.INT())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("phone", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

    @Test
    public void testIgnoreApplySchemaChange() throws IOException {
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(Duration.ofSeconds(1));
        try (SchemaRegistryRequestHandler schemaRegistryRequestHandler =
                newRequestHandler(metadataApplier, SchemaChangeBehavior.IGNORE)) {
            schemaRegistryRequestHandler.applySchemaChange(
                    CUSTOMERS,
                    Collections.singletonList(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)));
            Assertions.assertTrue(CollectionUtils.isEmpty(metadataApplier.getSchemaChangeEvents()));
        }
    }

    @Test
    public void testExceptionApplySchemaChange() throws IOException {
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(Duration.ofSeconds(1));

        try (SchemaRegistryRequestHandler schemaRegistryRequestHandler =
                newRequestHandler(metadataApplier, SchemaChangeBehavior.EXCEPTION)) {
            schemaRegistryRequestHandler.applySchemaChange(
                    CUSTOMERS,
                    Collections.singletonList(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)));
            Assertions.assertTrue(
                    schemaRegistryRequestHandler.schemaChangeException
                            instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testEvolveApplySchemaChange() throws IOException {
        CollectingMetadataApplier metadataApplier =
                new CollectingMetadataApplier(Duration.ofSeconds(1));
        try (SchemaRegistryRequestHandler schemaRegistryRequestHandler =
                newRequestHandler(metadataApplier, SchemaChangeBehavior.EVOLVE)) {
            schemaRegistryRequestHandler.applySchemaChange(
                    CUSTOMERS,
                    Collections.singletonList(new CreateTableEvent(CUSTOMERS, CUSTOMERS_SCHEMA)));
            Assertions.assertTrue(
                    CollectionUtils.isNotEmpty(metadataApplier.getSchemaChangeEvents()));
        }
    }

    private static SchemaRegistryRequestHandler newRequestHandler(
            CollectingMetadataApplier metadataApplier, SchemaChangeBehavior behavior) {
        SchemaManager schemaManager = new SchemaManager();
        return new SchemaRegistryRequestHandler(
                metadataApplier,
                schemaManager,
                new SchemaDerivation(schemaManager, Collections.emptyList(), new HashMap<>()),
                behavior);
    }
}
