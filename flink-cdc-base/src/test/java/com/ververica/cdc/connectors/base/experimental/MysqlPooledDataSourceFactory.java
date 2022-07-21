/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.experimental;

import org.apache.flink.annotation.Experimental;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;

/** A MySQL datasource factory. */
@Experimental
public class MysqlPooledDataSourceFactory extends JdbcConnectionPoolFactory {

    public static final String JDBC_URL_PATTERN =
            "jdbc:mysql://%s:%s/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL";

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();

        return String.format(JDBC_URL_PATTERN, hostName, port);
    }
}
