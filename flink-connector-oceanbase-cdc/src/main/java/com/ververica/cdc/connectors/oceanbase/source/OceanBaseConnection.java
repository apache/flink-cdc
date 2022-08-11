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

package com.ververica.cdc.connectors.oceanbase.source;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;

import java.time.Duration;

/** {@link JdbcConnection} extension to be used with OceanBase server. */
public class OceanBaseConnection extends JdbcConnection {

    protected static final String URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=convertToNull&connectTimeout=${connectTimeout}";
    protected static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

    public OceanBaseConnection(
            String hostname,
            Integer port,
            String user,
            String password,
            Duration timeout,
            ClassLoader classLoader) {
        super(config(hostname, port, user, password, timeout), factory(classLoader));
    }

    public static Configuration config(
            String hostname, Integer port, String user, String password, Duration timeout) {
        return Configuration.create()
                .with("hostname", hostname)
                .with("port", port)
                .with("user", user)
                .with("password", password)
                .with("connectTimeout", timeout == null ? 30000 : timeout.toMillis())
                .build();
    }

    public static JdbcConnection.ConnectionFactory factory(ClassLoader classLoader) {
        return JdbcConnection.patternBasedFactory(URL_PATTERN, DRIVER_CLASS_NAME, classLoader);
    }
}
