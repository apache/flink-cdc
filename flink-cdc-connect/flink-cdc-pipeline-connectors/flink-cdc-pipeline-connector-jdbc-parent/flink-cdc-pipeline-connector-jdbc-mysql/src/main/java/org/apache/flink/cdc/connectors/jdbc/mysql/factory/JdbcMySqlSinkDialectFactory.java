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

package org.apache.flink.cdc.connectors.jdbc.mysql.factory;

import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialect;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialectFactory;
import org.apache.flink.cdc.connectors.jdbc.mysql.dialect.MySqlJdbcSinkDialect;

/** A {@link JdbcSinkDialectFactory} for MySQL variants. */
public class JdbcMySqlSinkDialectFactory implements JdbcSinkDialectFactory<JdbcSinkConfig> {
    public static final String IDENTIFIER = "mysql";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public JdbcSinkDialect createDialect(JdbcSinkConfig config) {
        Preconditions.checkArgument(
                IDENTIFIER.equalsIgnoreCase(config.getDialect()),
                "JDBC sink with `%s` dialect doesn't work with specified dialect %s (inferred from connection URL: %s)",
                IDENTIFIER,
                config.getDialect(),
                config.getConnUrl());
        return new MySqlJdbcSinkDialect(IDENTIFIER, config);
    }
}
