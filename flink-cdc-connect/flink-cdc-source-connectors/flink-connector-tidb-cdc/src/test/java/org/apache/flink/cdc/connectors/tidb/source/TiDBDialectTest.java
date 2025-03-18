package org.apache.flink.cdc.connectors.tidb.source;

import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TiDBDialectTest extends TiDBTestBase {
    private static final String databaseName = "customer";
    private static final String tableName = "customers";

    @Test
    public void testDiscoverDataCollectionsInMultiDatabases() {
        initializeTidbTable("customer");
        TiDBSourceConfigFactory configFactoryOfCustomDatabase =
                getMockTiDBSourceConfigFactory(databaseName, null, tableName, 10);

        TiDBDialect dialectOfcustomDatabase =
                new TiDBDialect(configFactoryOfCustomDatabase.create(0));
        List<TableId> tableIdsOfcustomDatabase =
                dialectOfcustomDatabase.discoverDataCollections(
                        configFactoryOfCustomDatabase.create(0));
        Assert.assertEquals(tableIdsOfcustomDatabase.get(0).toString(), "customer.customers");
    }
}
