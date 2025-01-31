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

package org.apache.flink.cdc.connectors.oceanbase.source.connection;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.oceanbase.utils.OceanBaseUtils;

/** The OceanBase datasource factory. */
public class OceanBaseConnectionPoolFactory extends JdbcConnectionPoolFactory {

    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://%s:%s/?useUnicode=true&useSSL=false&useInformationSchema=true&nullCatalogMeansCurrent=false&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&characterSetResults=UTF-8";
    private static final String OB_URL_PATTERN =
            "jdbc:oceanbase://%s:%s/?useUnicode=true&useSSL=false&useInformationSchema=true&nullCatalogMeansCurrent=false&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&characterSetResults=UTF-8";

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        String driver = sourceConfig.getDriverClassName();
        if (OceanBaseUtils.isOceanBaseDriver(driver)) {
            return String.format(OB_URL_PATTERN, hostName, port);
        }
        return String.format(MYSQL_URL_PATTERN, hostName, port);
    }
}
