/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.jdbc.sink.v2;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.jdbc.catalog.Catalog;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Builder to construct {@link org.apache.flink.connector.jdbc.sink.JdbcSink}. */
@PublicEvolving
public class JdbcSinkBuilder<IN> {
    private JdbcExecutionOptions executionOptions;
    private Catalog catalog;
    private RecordSerializationSchema<IN> serializationSchema;

    public JdbcSinkBuilder() {
        this.executionOptions = JdbcExecutionOptions.defaults();
    }

    public JdbcSinkBuilder<IN> withExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = checkNotNull(executionOptions, "executionOptions cannot be null");
        return this;
    }

    public JdbcSinkBuilder<IN> withCatalog(Catalog catalog) {
        this.catalog = checkNotNull(catalog, "catalog cannot be null");
        return this;
    }

    public JdbcSinkBuilder<IN> withSerializationSchema(
            RecordSerializationSchema<IN> serializationSchema) {
        this.serializationSchema =
                checkNotNull(serializationSchema, "serializationSchema cannot be null");
        return this;
    }

    public JdbcSink<IN> buildAtLeastOnce(JdbcConnectionOptions connectionOptions) {
        checkNotNull(connectionOptions, "connectionOptions cannot be null");

        return buildAtLeastOnce(new SimpleJdbcConnectionProvider(connectionOptions));
    }

    public JdbcSink<IN> buildAtLeastOnce(JdbcConnectionProvider connectionProvider) {
        checkNotNull(connectionProvider, "connectionProvider cannot be null");

        return build(checkNotNull(connectionProvider, "connectionProvider cannot be null"));
    }

    private JdbcSink<IN> build(JdbcConnectionProvider connectionProvider) {

        return new JdbcSink<>(executionOptions, connectionProvider, serializationSchema, catalog);
    }
}
