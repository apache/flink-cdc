/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.flink.cdc.flink;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that this thin module compiles and runs tests against exactly its pinned Flink version
 * (2.2.0), and that Flink 2.x-only APIs are available on its classpath.
 */
class FlinkVersionIdentityTest {

    @Test
    void resolvesTheExactPinnedFlinkVersion() throws IOException {
        Properties versionProperties = new Properties();
        try (InputStream in =
                FlinkVersionIdentityTest.class.getResourceAsStream("/version.properties")) {
            versionProperties.load(in);
        }
        assertThat(versionProperties.getProperty("flink.version")).isEqualTo("2.2.0");
    }

    @Test
    void flink2xOnlyStateApiIsAvailable() throws ClassNotFoundException {
        assertThat(Class.forName("org.apache.flink.api.common.state.v2.MapStateDescriptor"))
                .isNotNull();
    }
}
