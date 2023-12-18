/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.composer.flink.translator;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSourceFactory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.source.DataSource;
import com.ververica.cdc.composer.definition.SourceDef;
import com.ververica.cdc.composer.utils.FactoryDiscoveryUtils;
import com.ververica.cdc.composer.utils.factory.DataSourceFactory1;
import org.junit.Assert;
import org.junit.Test;

/** A test for the {@link DataSourceTranslator}. */
public class DataSourceTranslatorTest {

    @Test
    public void testCreateDataSourceFromSourceDef() {
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

        Assert.assertTrue(dataSource instanceof DataSourceFactory1.TestDataSource);
        Assert.assertEquals("0.0.0.0", ((DataSourceFactory1.TestDataSource) dataSource).getHost());
    }
}
