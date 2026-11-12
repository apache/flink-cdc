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

package org.apache.flink.cdc.connectors.mongodb.source.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MongoUtils}. */
class MongoUtilsTest {

    @Test
    void shouldEncodeSpacesInCredentialsAsPercent20() {
        String connectionString =
                MongoUtils.buildConnectionString(
                        "my user", "my pass", "mongodb", "localhost:27017", null);

        assertThat(connectionString).isEqualTo("mongodb://my%20user:my%20pass@localhost:27017");
    }

    @Test
    void shouldBuildConnectionStringWithoutCredentials() {
        String connectionString =
                MongoUtils.buildConnectionString(null, null, "mongodb", "localhost:27017", null);

        assertThat(connectionString).isEqualTo("mongodb://localhost:27017");
    }

    @Test
    void shouldAppendConnectionOptions() {
        String connectionString =
                MongoUtils.buildConnectionString(
                        "user", "pass", "mongodb", "localhost:27017", "replicaSet=rs0");

        assertThat(connectionString)
                .isEqualTo("mongodb://user:pass@localhost:27017/?replicaSet=rs0");
    }
}
