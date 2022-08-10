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

package com.ververica.cdc.connectors.oracle.util;

import java.util.Properties;

/** The utils to build the JDBC connection url for Oracle data source. */
public class OracleJdbcUrlUtils {

    /**
     * Gets the URL in SID format.
     *
     * @param properties
     * @return jdbcUrl in SID format.
     */
    public static String getConnectionUrlWithSid(Properties properties) {
        String url;
        if (properties.containsKey("database.url")) {
            url = properties.getProperty("database.url");
        } else {
            String hostname = properties.getProperty("database.hostname");
            String port = properties.getProperty("database.port");
            String dbname = properties.getProperty("database.dbname");
            url = "jdbc:oracle:thin:@" + hostname + ":" + port + ":" + dbname;
        }
        return url;
    }
}
