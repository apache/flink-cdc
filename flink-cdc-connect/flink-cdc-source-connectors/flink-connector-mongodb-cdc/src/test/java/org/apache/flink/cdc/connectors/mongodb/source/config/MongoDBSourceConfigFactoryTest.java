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

package org.apache.flink.cdc.connectors.mongodb.source.config;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MongoDBSourceConfigFactory}. */
class MongoDBSourceConfigFactoryTest {

    @Test
    void testFailFastWhenReleaseAndNewlyAddedTableBothEnabled() {
        // Enabling metadata release together with scan.newly-added-table is contradictory: the
        // release would drop the metadata the newly-added-table flow needs. The config must reject
        // it at build time rather than silently disabling the release.
        MongoDBSourceConfigFactory factory = new MongoDBSourceConfigFactory();
        factory.hosts("localhost:27017")
                .scanNewlyAddedTableEnabled(true)
                .releaseSnapshotMetadataEnabled(true);

        Assertions.assertThatThrownBy(() -> factory.create(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot both be enabled");
    }

    @Test
    void testReleaseSnapshotMetadataIsDisabledByDefault() {
        MongoDBSourceConfig config =
                new MongoDBSourceConfigFactory().hosts("localhost:27017").create(0);

        Assertions.assertThat(config.isReleaseSnapshotMetadataEnabled()).isFalse();
    }

    @Test
    void testReleaseSnapshotMetadataReachesTheConfig() {
        // the enumerator reads this getter to decide whether to arm the release
        MongoDBSourceConfig config =
                new MongoDBSourceConfigFactory()
                        .hosts("localhost:27017")
                        .releaseSnapshotMetadataEnabled(true)
                        .create(0);

        Assertions.assertThat(config.isReleaseSnapshotMetadataEnabled()).isTrue();
    }
}
