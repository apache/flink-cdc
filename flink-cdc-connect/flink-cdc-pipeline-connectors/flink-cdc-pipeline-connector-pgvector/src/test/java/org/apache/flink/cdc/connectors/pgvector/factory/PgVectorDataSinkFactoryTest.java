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

package org.apache.flink.cdc.connectors.pgvector.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.pgvector.sink.PgVectorDataSink;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link PgVectorDataSinkFactory}. */
class PgVectorDataSinkFactoryTest {

    @Test
    void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Assertions.assertThat(sinkFactory).isInstanceOf(PgVectorDataSinkFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:postgresql://127.0.0.1:5432/postgres")
                                .put("username", "postgres")
                                .put("password", "postgres")
                                .put("vector.columns.public.docs.embedding", "vector(3)")
                                .put("table.create.properties.fillfactor", "90")
                                .build());

        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSink).isInstanceOf(PgVectorDataSink.class);
    }

    @Test
    void testMissingRequiredOption() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.of("jdbc-url", "jdbc:postgresql://127.0.0.1:5432/postgres"));

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("username");
    }

    @Test
    void testRejectInvalidVectorColumnDefinition() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:postgresql://127.0.0.1:5432/postgres")
                                .put("username", "postgres")
                                .put("vector.columns.public.docs.embedding", "float[]")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid vector column definition");
    }

    @Test
    void testRejectInvalidJdbcUrl() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:mysql://127.0.0.1:5432/postgres")
                                .put("username", "postgres")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("jdbc:postgresql");
    }

    @Test
    void testRejectInvalidTableCreatePropertyKey() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:postgresql://127.0.0.1:5432/postgres")
                                .put("username", "postgres")
                                .put(
                                        "table.create.properties.fillfactor); DROP TABLE docs; --",
                                        "90")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid PostgreSQL table create property key");
    }

    @Test
    void testRejectNonPositiveFlushInterval() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:postgresql://127.0.0.1:5432/postgres")
                                .put("username", "postgres")
                                .put("sink.flush.interval", "0s")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.flush.interval");
    }

    @Test
    void testRejectNegativeRetryBackoff() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("pgvector", DataSinkFactory.class);
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:postgresql://127.0.0.1:5432/postgres")
                                .put("username", "postgres")
                                .put("sink.retry.backoff", "-1s")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sinkFactory.createDataSink(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink.retry.backoff");
    }
}
