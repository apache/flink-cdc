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

package org.apache.flink.cdc.composer.utils;

import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.composer.utils.factory.DataSinkFactory1;
import org.apache.flink.cdc.composer.utils.factory.DataSourceFactory1;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FactoryDiscoveryUtils}. */
class FactoryDiscoveryUtilsTest {

    @Test
    void getFactoryByIdentifier() {
        assertThat(
                        FactoryDiscoveryUtils.getFactoryByIdentifier(
                                "data-source-factory-1", Factory.class))
                .isInstanceOf(DataSourceFactory1.class);

        assertThat(
                        FactoryDiscoveryUtils.getFactoryByIdentifier(
                                "data-sink-factory-1", Factory.class))
                .isInstanceOf(DataSinkFactory1.class);

        assertThatThrownBy(
                        () ->
                                FactoryDiscoveryUtils.getFactoryByIdentifier(
                                        "data-sink-factory-3", Factory.class))
                .hasMessageStartingWith(
                        "Cannot find factory with identifier \"data-sink-factory-3\" in the classpath");
    }
}
