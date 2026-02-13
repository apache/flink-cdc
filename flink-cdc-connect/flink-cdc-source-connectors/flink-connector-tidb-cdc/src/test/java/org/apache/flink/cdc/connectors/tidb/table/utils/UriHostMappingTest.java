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

package org.apache.flink.cdc.connectors.tidb.table.utils;

import org.apache.flink.cdc.connectors.tidb.TDBSourceOptions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.tikv.common.TiConfiguration;

import java.util.HashMap;

/** Unit test for {@link UriHostMapping}. * */
class UriHostMappingTest {

    @Test
    void uriHostMappingTest() {
        final TiConfiguration tiConf =
                TDBSourceOptions.getTiConfiguration(
                        "http://0.0.0.0:2347", "host1:1;host2:2;host3:3", new HashMap<>());
        UriHostMapping uriHostMapping = (UriHostMapping) tiConf.getHostMapping();
        Assertions.assertThat(uriHostMapping.getHostMapping())
                .hasSize(3)
                .containsEntry("host1", "1");
    }

    @Test
    void uriHostMappingEmpty() {
        final TiConfiguration tiConf =
                TDBSourceOptions.getTiConfiguration("http://0.0.0.0:2347", "", new HashMap<>());
        UriHostMapping uriHostMapping = (UriHostMapping) tiConf.getHostMapping();
        Assertions.assertThat(uriHostMapping.getHostMapping()).isNull();
    }

    @Test
    void uriHostMappingError() {
        Assertions.assertThatThrownBy(
                        () ->
                                TDBSourceOptions.getTiConfiguration(
                                        "http://0.0.0.0:2347",
                                        "host1=1;host2=2;host3=3",
                                        new HashMap<>()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid host mapping string: host1=1;host2=2;host3=3");
    }
}
