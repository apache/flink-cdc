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

package com.ververical.cdc.connectors.starrocks.sink;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.factories.FactoryHelper;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.composer.utils.FactoryDiscoveryUtils;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSink;
import com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Tests for {@link StarRocksDataSinkFactory}. */
public class StarRocksDataSinkFactoryTest {

    @Test
    public void testCreateDataSink() {
        DataSinkFactory sinkFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("starrocks", DataSinkFactory.class);
        assertTrue(sinkFactory instanceof StarRocksDataSinkFactory);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
                                .put("load-url", "127.0.0.1:8030")
                                .put("username", "root")
                                .put("password", "")
                                .build());
        DataSink dataSink =
                sinkFactory.createDataSink(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        assertTrue(dataSink instanceof StarRocksDataSink);
    }
}
