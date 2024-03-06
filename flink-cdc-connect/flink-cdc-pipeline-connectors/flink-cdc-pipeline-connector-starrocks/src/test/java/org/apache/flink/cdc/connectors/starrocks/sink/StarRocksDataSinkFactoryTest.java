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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Tests for {@link org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkFactory}. */
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
