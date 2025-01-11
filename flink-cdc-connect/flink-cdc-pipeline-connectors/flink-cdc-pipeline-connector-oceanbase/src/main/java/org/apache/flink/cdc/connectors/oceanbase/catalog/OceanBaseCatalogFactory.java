/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase.catalog;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.dialect.OceanBaseDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseMySQLDialect;
import com.oceanbase.connector.flink.dialect.OceanBaseOracleDialect;

/** A {@link OceanBaseCatalogFactory} to create {@link OceanBaseCatalog}. */
public class OceanBaseCatalogFactory {

    public static OceanBaseCatalog createOceanBaseCatalog(
            OceanBaseConnectorOptions connectorOptions) throws Exception {
        try (OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions)) {
            OceanBaseDialect dialect = connectionProvider.getDialect();
            if (dialect instanceof OceanBaseMySQLDialect) {
                return new OceanBaseMySQLCatalog(connectorOptions);
            } else if (dialect instanceof OceanBaseOracleDialect) {
                return new OceanBaseOracleCatalog(connectorOptions);
            } else {
                throw new OceanBaseCatalogException(
                        "Fail to create OceanBaseCatalog: unknown tenant.");
            }
        }
    }
}
