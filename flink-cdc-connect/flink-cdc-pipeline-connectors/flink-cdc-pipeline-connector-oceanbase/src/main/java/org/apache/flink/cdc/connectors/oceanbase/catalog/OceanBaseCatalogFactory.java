package org.apache.flink.cdc.connectors.oceanbase.catalog;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;
import org.apache.flink.cdc.common.factories.DataSinkFactory;

/** A {@link OceanBaseCatalogFactory} to create {@link OceanBaseCatalog}. */
public class OceanBaseCatalogFactory {

    public static OceanBaseCatalog createOceanBaseCatalog(OceanBaseConnectorOptions connectorOptions) throws Exception {
        try (OceanBaseConnectionProvider connectionProvider = new OceanBaseConnectionProvider(connectorOptions)) {
            OceanBaseDialect dialect = connectionProvider.getDialect();
            if (dialect instanceof OceanBaseMySQLDialect) {
                return new OceanBaseMySQLCatalog(connectorOptions);
            } else if (dialect instanceof OceanBaseOracleDialect) {
                return new OceanBaseOracleCatalog(connectorOptions);
            }else {
                throw new OceanBaseCatalogException("This tenant is not supported currently");
            }
        }
    }
}
