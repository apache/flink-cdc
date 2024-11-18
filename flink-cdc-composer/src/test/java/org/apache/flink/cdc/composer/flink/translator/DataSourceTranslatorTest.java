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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.composer.utils.factory.DataSourceFactory1;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** A test for the {@link DataSourceTranslator}. */
class DataSourceTranslatorTest {

    @Test
    void testCreateDataSourceFromSourceDef() {
        SourceDef sourceDef =
                new SourceDef(
                        "data-source-factory-1",
                        "source-database",
                        Configuration.fromMap(
                                ImmutableMap.<String, String>builder()
                                        .put("host", "0.0.0.0")
                                        .build()));

        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier(
                        sourceDef.getType(), DataSourceFactory.class);

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                sourceDef.getConfig(),
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()));

        Assertions.assertThat(dataSource)
                .isExactlyInstanceOf(DataSourceFactory1.TestDataSource.class);
        Assertions.assertThat(((DataSourceFactory1.TestDataSource) dataSource).getHost())
                .isEqualTo("0.0.0.0");
    }
}
